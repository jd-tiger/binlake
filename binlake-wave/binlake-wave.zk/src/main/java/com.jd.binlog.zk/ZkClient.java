package com.jd.binlog.zk;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.binlog.alarm.AlarmUtils;
import com.jd.binlog.alarm.utils.JDPhoneParas;
import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.config.bean.ServerConfig;
import com.jd.binlog.config.bean.ZKConfig;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.inter.work.IWorkInitializer;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Http;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.meta.MetaUtils;
import com.jd.binlog.util.ConstUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by pengan on 16-12-15.
 */
public class ZkClient implements IZkClient {
    // configuration
    private final String host;
    private final ZKConfig zkConfig;
    private final ServerConfig sc;
    private final HttpConfig hc;

    // interface
    private final IWorkInitializer workInit; // 初始化work 工具

    // zookeeper related
    private final CuratorFramework client;
    private PathChildrenCache childrenCache;

    // leader selector map
    private final ConcurrentHashMap<String, ILeaderSelector> lsm;

    public ZkClient(ZKConfig zkConfig,
                    ServerConfig sc,
                    HttpConfig hc,
                    IWorkInitializer initI,
                    ConcurrentHashMap<String, ILeaderSelector> lsm) {
        this.sc = sc;
        this.hc = hc;
        this.lsm = lsm;
        this.workInit = initI;
        this.zkConfig = zkConfig;
        this.host = sc.getHost();
        this.client = CuratorFrameworkFactory.newClient(
                zkConfig.getServers(),
                new RetryNTimes(zkConfig.getRetryTimes(), zkConfig.getSleepMsBetweenRetries())
        );
    }

    public void start() throws Exception {
        this.client.start();

        // 初始化 zk 监听路径
        createMetaPathIfNotExists();

        // init alarm utils
        String p = this.zkConfig.getMetaPath().substring(this.zkConfig.getMetaPath().indexOf("/")) + ConstUtils.ZK_ALARM_PATH;
        Meta.Admin admin = Meta.Admin.unmarshalJson(client.getData().forPath(p));
        AlarmUtils.init(admin.getMailParas(), admin.getAdminMails(), admin.getPhoneParas(), admin.getAdminPhones());

        addConnectionStateListener(this.client, this.zkConfig);
        addChildrenListener(this.client, this.zkConfig);
        new ZkConnRecycler().start(); // 启动回收线程
        new ZkPathRemover(zkConfig.getServers()).start(); // 启动 路径删除 线程
    }

    /**
     * 创建 监听路径
     *
     * @throws Exception
     */
    private void createMetaPathIfNotExists() throws Exception {
        String path = zkConfig.getMetaPath();
        if (client.checkExists().forPath(path) == null) {
            String prefix = "";
            for (String name : path.split("/")) {
                prefix = prefix + "/" + name;
                if (prefix.equals("/")) {
                    prefix = "";
                    continue; // 仅仅是根路径
                }

                if (client.checkExists().forPath(prefix) == null) {
                    client.create().forPath(prefix, prefix.getBytes());
                }
            }
        }
    }

    /**
     * add connection state listener for connection state changed
     *
     * @param client
     * @param zkConfig
     */
    private void addConnectionStateListener(CuratorFramework client, final ZKConfig zkConfig) {
        client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
            public void stateChanged(CuratorFramework client, ConnectionState newState) {
                LogUtils.info.info("parent path connection state changed to " + newState
                        + " for path " + zkConfig.getMetaPath());
                switch (newState) {
                    case RECONNECTED:
                        abandonLeader();
                        addAll(client, zkConfig);
                        break;
                    case SUSPENDED:
                    case LOST:
                        abandonLeader();
                }
            }

            private void addAll(CuratorFramework client, ZKConfig zkConfig) {
                LogUtils.info.info("add ephemeral node to all MySQL ZNode");
                try {
                    childrenCache.rebuild();
                    for (ChildData childData : childrenCache.getCurrentData()) {
                        try {
                            addEphemeralNode(client, childData, zkConfig);
                        } catch (Throwable exp) {
                            LogUtils.error.error("add ephemeral sequential node error : " + childData.getPath(), exp);
                        }
                    }
                } catch (Exception e) {
                    LogUtils.error.error("add all listener error", e);
                }
            }

            /**
             * 链接已经断开 需要关闭cache当中所有节点的selector
             */
            private void abandonLeader() {
                LogUtils.info.info("close all selector because of the zk connection lost");
                try {
                    List<String> keys = new LinkedList<String>();
                    for (Map.Entry<String, ILeaderSelector> entry : lsm.entrySet()) {
                        String key = entry.getKey();
                        LogUtils.info.info("close connect : " + key);
                        ILeaderSelector leader = entry.getValue();
                        if (leader.getLeaderSelector().hasLeadership()) {
                            leader.getWork().close();
                            leader.abandonLeaderShip();
                        }
                        keys.add(key);
                    }

                    for (String key : keys) {
                        // 删除 map 当中 key 对应值
                        lsm.remove(key);
                    }

                } catch (Exception e) {
                    LogUtils.error.error("get children for close error : " + zkConfig.getMetaPath(), e);
                }
            }
        });
    }


    /**
     * add children listener for zk path
     *
     * @param client
     * @param zkConfig
     * @throws Exception
     */
    private void addChildrenListener(CuratorFramework client, final ZKConfig zkConfig) throws Exception {
        childrenCache = new PathChildrenCache(client, zkConfig.getMetaPath(), true);
        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                ChildData data = event.getData();

                if (data != null) {
                    switch (event.getType()) {
                        case CHILD_ADDED:
                            addEphemeralNode(client, data, zkConfig);
                            break;
                        case CHILD_REMOVED:
                            closeWork(data);
                            break;
                        case CONNECTION_SUSPENDED:
                        case CONNECTION_LOST:
                            closeDump(data);
                            break;
                        case CHILD_UPDATED:
                            // check node state {online or offline}
                            updateState(client, data, zkConfig);
                            break;
                        default:
                            break;
                    }
                }
            }
        };
        childrenCache.getListenable().addListener(pathChildrenCacheListener);
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }

    /**
     * update MySQL node:
     * mainly focus on state change
     * close the previous node if exist
     *
     * @param client
     * @param data
     * @param zkConfig
     * @throws Exception
     */
    private void updateState(CuratorFramework client, ChildData data, ZKConfig zkConfig) throws Exception {
        LogUtils.debug.debug("updateState");

        try {
            Meta.DbInfo dbInfo = Meta.DbInfo.unmarshalJson(data.getData());

            switch (dbInfo.getState()) {
                case OFFLINE:
                    try {
                        refreshCurrentLogPos(data);
                    } catch (Exception e) {
                        LogUtils.error.error("refresh current binlog offset error", e);
                    } finally {
                        closeWork(data);
                    }
                    break;
                case ONLINE:
                    closeWork(data);
                    try {
                        addEphemeralNode(client, data, zkConfig);
                    } catch (Throwable exp) {
                        LogUtils.error.error("addEphemeralNode error: " + data.getPath(), exp);
                        closeWork(data);
                    }
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
        }
    }

    /**
     * add ephemeral node to MySQL znode
     *
     * @param client
     * @param data
     * @param zkConfig
     * @throws Exception
     */
    private void addEphemeralNode(CuratorFramework client, ChildData data, ZKConfig zkConfig) throws Exception {
        String key = getChildDataKey(data);

        if (lsm.containsKey(key)) {
            LogUtils.warn.warn("key: " + key + " already exist in leaderMap");
            return;
        }

        String path = data.getPath() + ConstUtils.ZK_DYNAMIC_PATH;
        String counterPath = data.getPath() + ConstUtils.ZK_COUNTER_PATH;

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("addEphemeralNode path :" + path);
        }

        byte[] binLogByte = client.getData().forPath(path);
        byte[] counterBytes = client.getData().forPath(counterPath);
        Meta.DbInfo dbInfo = MetaUtils.dbInfo(client, data.getPath());
        Meta.BinlogInfo binlogInfo = Meta.BinlogInfo.unmarshalJson(binLogByte);
        Meta.Counter counter = Meta.Counter.unmarshalJson(counterBytes);
        MetaInfo metaInfo = new MetaInfo(dbInfo, binlogInfo, counter);

        String candidatePath = data.getPath() + ConstUtils.ZK_CANDIDATE_PATH;
        if (client.checkExists().forPath(candidatePath) != null) {
            byte[] candidateBytes = client.getData().forPath(candidatePath);
            Meta.Candidate candidate = Meta.Candidate.unmarshalJson(candidateBytes);
            if (candidate.getHost() != null && !candidate.getHost().contains(host)) {
                LogUtils.warn.warn("local host " + host + " is not contained in candidate hosts, not add ephemeral node");
                return;
            }
        }

        if (metaInfo.getRetryTimes() > sc.getDumpLatch()) {
            // session retry times is over than latch, just abandon to create listener on this node
            LogUtils.info.info("MySQL ZNode " + metaInfo.getHost() + ":" + metaInfo.getPort() + " retryTimes : " + metaInfo.getRetryTimes() + " > " +
                    sc.getDumpLatch());
            return;
        }

        switch (dbInfo.getState()) {
            case OFFLINE:
                LogUtils.warn.warn("host: " + metaInfo.getHost() + " is offline");
                return;
        }

        LogUtils.debug.debug(binlogInfo);

        // start current leader
        ILeaderSelector lsi = new ZkLeaderSelector(data.getPath(),
                key, zkConfig, sc, hc, workInit);
        lsi.startSelector();

        // previous leader
        ILeaderSelector pl = lsm.putIfAbsent(getChildDataKey(data), lsi);

        if (pl != null) {
            pl.close();
        }
    }

    /**
     * 带上时间戳是为防止 删除立马又新建情况下
     * <p>
     * 本应该删除老的却删除了新的节点 出现数据不对
     *
     * @param data
     * @return
     */
    private String getChildDataKey(ChildData data) {
        return data.getPath() + ":" + data.getStat().getCtime();
    }

    /**
     * 与zk的所有链接 如果发生异常 需要关闭所有链接 包括zk的链接
     *
     * @param data
     * @throws Exception
     */
    private void closeWork(ChildData data) {
        LogUtils.debug.debug("closeWork");
        /**
         * 需要放弃leader的线程 future.cancel（）
         */
        String leaderKey = getChildDataKey(data);
        ILeaderSelector ls = lsm.remove(leaderKey);

        if (ls != null) {
            ls.close();
        }
    }

    /**
     * 关闭dump线程 退出leader 但是不close 临时节点
     *
     * @param data
     * @throws Exception
     */
    private void closeDump(ChildData data) {
        LogUtils.debug.debug("closeWork");
        /**
         * 需要放弃leader的线程 future.cancel（）
         */
        String leaderKey = getChildDataKey(data);
        ILeaderSelector ls = lsm.get(leaderKey);

        if (ls != null) {
            IBinlogWorker work = null;

            if ((work = ls.getWork()) != null) {
                work.close();
            }
            if (ls.getLeaderSelector().hasLeadership()) {
                ls.abandonLeaderShip();
            }
        }
    }

    /**
     * 设置offline 强制将最新的binlog位置刷新到 zookeeper 服务
     *
     * @param data
     * @throws Exception
     */
    private void refreshCurrentLogPos(ChildData data) throws Exception {
        String leaderKey = getChildDataKey(data);
        ILeaderSelector ls = null;

        if ((ls = lsm.get(leaderKey)) != null) {
            ls.refreshLogPos();
        }
    }

    @Override
    public void addLeaderSelector(Http.RefreshRequest req) throws Exception {
        // mysql instance path
        String dbp = zkConfig.getMetaPath() + File.separator + req.getKey();
        ChildData dbBts = this.childrenCache.getCurrentData(dbp);
        Meta.DbInfo db = Meta.DbInfo.unmarshalJson(dbBts.getData());

        // binlog path
        String bp = dbp + File.separator + ConstUtils.ZK_DYNAMIC_PATH;
        ChildData binlogBts = this.childrenCache.getCurrentData(bp);
        Meta.BinlogInfo binlog = Meta.BinlogInfo.unmarshalJson(binlogBts.getData());

        // 判断元数据版本
        if (db.getMetaVersion() <= req.getMetaVersion() && binlog.getLeaderVersion() <= req.getLeaderVersion()) {
            // make sure the meta of the same version and leader version is not bigger than request
            addEphemeralNode(this.client, dbBts, this.zkConfig); // 最后添加 因为如果开始添加可能获取的candidate 不一致
        } else {
            String errMsg = "can't add ephemeral node because leader have changed for path " + dbp + " or because the meta was updated by manager";
            LogUtils.error.error(errMsg);
            AlarmUtils.phone(JDPhoneParas.phoneParas(String.format("wave %s add ephemeral node for %s failure: %s", host, req.getKey()).getBytes()));
            throw new Exception(errMsg);
        }
    }
}
