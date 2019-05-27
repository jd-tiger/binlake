package com.jd.binlake.tower.zk;

import com.jd.binlake.tower.api.ApiCenter;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaUtils;
import com.jd.binlog.util.ConstUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by pengan on 17-3-3.
 * <p>
 * Zk service using for create node like {host:port, candidates, etc. }
 */
public class ZkService {
    private static final Logger logger = Logger.getLogger(ZkService.class);
    private static final int RETRY_TIMES = 3;
    private static final int SLEEP_MS_BETWEEN_RETRIES = 6000;

    // share the same servers path dynamic
    private String zkPath;
    private CuratorFramework client;

    public ZkService(String servers, String path) {
        zkPath = path.endsWith("/") ? path : path + "/";
        client = CuratorFrameworkFactory.newClient(servers,
                new RetryNTimes(RETRY_TIMES, SLEEP_MS_BETWEEN_RETRIES));
        client.start();
    }

    /**
     * get DbInfo all get from client {manager client}
     *
     * @param host
     * @return
     * @throws Exception
     */
    public Meta.DbInfo getDbInfo(String host) throws Exception {
        logger.info("getDbInfo host : " + host);
        byte[] data = client.getData().forPath(zkPath + host);
        return Meta.DbInfo.unmarshalJson(data);
    }

    /**
     * update db info in case of the update for db info
     *
     * @param dbInfo
     * @throws Exception
     */
    public void setDbInfo(Meta.DbInfo dbInfo) throws Exception {
        logger.info("set dbInfo : " + dbInfo);
        String dbPath = zkPath + ApiCenter.makeZNodePath(dbInfo.getHost(), dbInfo.getPort() + "");

        // old db key values
        Map<String, byte[]> oldDbKVs = MetaUtils.dbBytesMap(this.zkPath, MetaUtils.dbInfo(client, dbPath));

        // new db key values
        Map<String, byte[]> newDbKVs = MetaUtils.dbBytesMap(this.zkPath, dbInfo);

        // deleted paths
        List<String> delPaths = new LinkedList<>();
        // new db key values
        for (Map.Entry<String, byte[]> entry : oldDbKVs.entrySet()) {
            String p = entry.getKey();
            if (!newDbKVs.containsKey(p)) {
                // not contain in new db kv => need to deleted
                delPaths.add(p);
            }

            if (newDbKVs.containsKey(p) && Arrays.equals(entry.getValue(), newDbKVs.get(p))) {
                // remove equals bytes values
                newDbKVs.remove(p);
            }
        }

        if (newDbKVs.size() == 0 && delPaths.size() == 0) {
            logger.warn("no need rules to add and no rule to delete");
            return;
        }

        setOffline(dbInfo.getHost(), dbInfo.getPort());

        refreshDbInfo(client, delPaths, newDbKVs);

        setOnline(dbInfo.getHost(), dbInfo.getPort());
    }

    /**
     * @param c:        curator client
     * @param delPaths: path to delete
     * @param newDbKVs: new db key values
     * @throws Exception
     */
    private void refreshDbInfo(CuratorFramework c, List<String> delPaths, Map<String, byte[]> newDbKVs) throws Exception {
        if (delPaths.size() == 0 && newDbKVs.size() == 0) {
            return;
        }

        // transaction
        CuratorTransaction trx = c.inTransaction();
        CuratorTransactionFinal trxf = null;

        for (String p : delPaths) {
            trxf = trx.delete().forPath(p).and();
        }

        for (Map.Entry<String, byte[]> entry : newDbKVs.entrySet()) {
            if (c.checkExists().forPath(entry.getKey()) == null) {
                // node not exist then create
                if (logger.isDebugEnabled()) {
                    logger.warn("path " + entry.getKey() + " not exit under zookeeper node " + zkPath);
                }
                trxf = trx.create().forPath(entry.getKey(), entry.getValue()).and();
                continue;
            }
            trxf = trx.setData().forPath(entry.getKey(), entry.getValue()).and();
        }

        trxf.commit();
    }

    /**
     * this is to refresh in cronTask
     *
     * @param host
     * @return
     * @throws Exception
     */
    public Meta.BinlogInfo getBinlogInfo(String host) throws Exception {
        logger.info("getBinlogInfo host: " + host);
        byte[] data = client.getData().forPath(zkPath + host + ConstUtils.ZK_DYNAMIC_PATH);
        return Meta.BinlogInfo.unmarshalJson(data);
    }

    public Meta.Counter getCounter(String host) throws Exception {
        logger.info("getCounter host : " + host);
        byte[] data = client.getData().forPath(zkPath + host + ConstUtils.ZK_COUNTER_PATH);
        return Meta.Counter.unmarshalJson(data);
    }

    public Meta.Candidate getCandidate(String host) throws Exception {
        logger.info("getCandidate host : " + host);
        byte[] data = client.getData().forPath(zkPath + host + ConstUtils.ZK_CANDIDATE_PATH);
        return Meta.Candidate.unmarshalJson(data);
    }

    public void close() {
        logger.info("close zk client");
        CuratorFramework client = this.client;
        if (client != null) {
            client.close();
        }
        this.client = null;
    }

    /**
     * 批量创建MySQL dump实例
     *
     * @param ms : meta data list
     * @throws Exception
     */
    public void batchCreate(List<Meta.MetaData> ms) throws Exception {
        if (ms.size() == 0) {
            logger.warn("no meta info exist on creating znode ");
            return;
        }

        // each time we push one into transaction for share
        Meta.Counter counter = new Meta.Counter().setKillTimes(0).setRetryTimes(0);
        byte[] cbts = Meta.Counter.marshalJson(counter);

        // transaction
        CuratorTransaction trx = client.inTransaction();
        CuratorTransactionFinal trxf = null;

        for (Meta.MetaData metaInfo : ms) {
            // parent path
            String path = zkPath + ApiCenter.makeZNodePath(metaInfo.getDbInfo().getHost(),
                    metaInfo.getDbInfo().getPort() + "");

            // create db info node
            Meta.DbInfo dbInfo = metaInfo.getDbInfo();
            for (Map.Entry<String, byte[]> entry : MetaUtils.dbBytesMap(zkPath, dbInfo).entrySet()) {
                logger.debug("create path for " + entry.getKey());
                trxf = trx.create().forPath(entry.getKey(), entry.getValue()).and();
            }

            // other nodes
            Meta.Candidate candidate = new Meta.Candidate().setHost(metaInfo.getCandidate());
            trxf = trx.create().forPath(path + ConstUtils.ZK_DYNAMIC_PATH, Meta.BinlogInfo.marshalJson(metaInfo.getSlave()))
                    .and().create().forPath(path + ConstUtils.ZK_COUNTER_PATH, cbts)
                    .and().create().forPath(path + ConstUtils.ZK_ERROR_PATH, Meta.Error.marshalJson(Meta.Error.defalut()))
                    .and().create().forPath(path + ConstUtils.ZK_ALARM_PATH, Meta.Alarm.marshalJson(metaInfo.getAlarm()))
                    .and().create().forPath(path + ConstUtils.ZK_CANDIDATE_PATH, Meta.Candidate.marshalJson(candidate)).and();
        }

        // commit transaction final
        trxf.commit();
    }


    /**
     * 检查 MySQL host 是否存在
     *
     * @param host
     * @param port
     * @return
     * @throws Exception
     */
    public boolean hostExist(String host, String port) throws Exception {
        logger.info("hostExist check: " + host + ": " + port);
        String key = ApiCenter.makeZNodePath(host, port);

        try {
            if (client.checkExists().forPath(zkPath + key) == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("connect zookeeper error : " + key + "\n" + e);
            throw e;
        }
        return true;
    }

    /***
     * path exist in zk which is parallel with /zk/wave3 eg. /zk/admin
     */
    public boolean nodeExist(String p) throws Exception {
        logger.info("admin node exist");

        try {
            if (client.checkExists().forPath(p) == null) {
                return false;
            }
        } catch (Exception e) {
            logger.error("connect zookeeper error : " + p + "\n" + e);
            throw e;
        }
        return true;
    }

    /**
     * 检查是否还有临时节点
     *
     * @param host
     * @param port
     * @return
     * @throws Exception
     */
    public boolean checkChildSENodeExist(String host, int port) throws Exception {
        String key = ApiCenter.makeZNodePath(host, port + "");
        List<String> childList = client.getChildren().forPath(zkPath + key);
        childList.remove(ConstUtils.ZK_DYNAMIC_PATH.substring(1));
        childList.remove(ConstUtils.ZK_COUNTER_PATH.substring(1));
        childList.remove(ConstUtils.ZK_TERMINAL_PATH.substring(1));
        childList.remove(ConstUtils.ZK_CANDIDATE_PATH.substring(1));
        childList.remove(ConstUtils.ZK_LEADER_PATH.substring(1));

        return childList.size() > 0;
    }

    /**
     * resetCounter 重置计数器 直接进行leader 选举
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void resetCounter(String host, int port) throws Exception {
        logger.debug("resetCounter host : " + host + ", port: " + port);
        String key = ApiCenter.makeZNodePath(host, port + "");
        byte[] dbData = client.getData().forPath(zkPath + key);
        CuratorTransaction tx = client.inTransaction();

        tx.setData().forPath(zkPath + key, dbData).and()
                .setData().forPath(zkPath + key + ConstUtils.ZK_COUNTER_PATH,
                Meta.Counter.marshalJson(new Meta.Counter())).and()
                .commit();
    }

    /**
     * 设置当前节点 下线 wait = true
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void setOffline(String host, int port) throws Exception {
        setOffline(host, port, true);
    }

    public void setOffline(String host, int port, boolean wait) throws Exception {
        logger.debug("set node status off : " + host + ":" + port);
        String key = ApiCenter.makeZNodePath(host, port + "");
        byte[] dbData = client.getData().forPath(zkPath + key);
        Meta.DbInfo dbInfo = Meta.DbInfo.unmarshalJson(dbData);

        switch (dbInfo.getState()) {
            case OFFLINE: // 如果是offline 则直接返回
                return;
        }

        logger.debug("dbinfo : " + dbInfo.toString());
        dbInfo = dbInfo.setState(Meta.NodeState.OFFLINE);
        logger.debug("new dbinfo : " + dbInfo.toString());

        client.setData().forPath(zkPath + key, Meta.DbInfo.marshalJson(dbInfo));

        if (wait) {
            int count = 0;
            while (true) {
                Thread.sleep(100);
                if (!checkChildSENodeExist(host, port)) {
                    break;
                }
                if (count > 10) {
                    throw new Exception("ephemeral node disappeared too long after set offline");
                }
                count++;
            }
        }
    }

    /**
     * 设置当前节点上线
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void setOnline(String host, int port) throws Exception {
        logger.debug("set node status on : " + host + ":" + port);
        String key = ApiCenter.makeZNodePath(host, port + "");
        byte[] dbData = client.getData().forPath(zkPath + key);
        Meta.DbInfo dbInfo = Meta.DbInfo.unmarshalJson(dbData);

        switch (dbInfo.getState()) {
            case ONLINE: // 已经是online 则放弃更新
                break;
        }

        logger.debug("dbinfo : " + dbInfo.toString());
        dbInfo = dbInfo.setState(Meta.NodeState.ONLINE);
        logger.debug("new dbinfo : " + dbInfo.toString());

        client.setData().forPath(zkPath + key, Meta.DbInfo.marshalJson(dbInfo));
    }

    /**
     * 设置binlog 位置
     * <p>
     * 也可以使用事務使得節點重新生效
     * <p>
     * 第一步: 设置binlog 位置
     * 第二步: 设置节点offline
     * 第三步: 设置节点启动
     * </p>
     *
     * @param host
     * @param port
     * @param binlogFile
     * @param binlogPosition
     * @param gtidSets
     * @throws Exception
     */
    public void setBinlogPosition(String host, int port, String binlogFile, long binlogPosition, String gtidSets) throws Exception {
        logger.debug("set binlog position on : " + host + ":" + port);
        String binlogPath = zkPath + ApiCenter.makeZNodePath(host, port + "") + ConstUtils.ZK_DYNAMIC_PATH;

        byte[] binlogData = client.getData().forPath(binlogPath);
        Meta.BinlogInfo binlogInfo = Meta.BinlogInfo.unmarshalJson(binlogData);

        binlogInfo = binlogInfo.setBinlogWhen(0).setBinlogFile(binlogFile).setBinlogPos(binlogPosition).setExecutedGtidSets(gtidSets);
        logger.debug("new binlog info  : " + binlogInfo.toString());

        // set offline
        setOffline(host, port);

        client.setData().forPath(binlogPath, Meta.BinlogInfo.marshalJson(binlogInfo));

        setOnline(host, port);
    }

    /**
     * 设置候选节点
     *
     * @param host
     * @param port
     * @param candidateHosts
     * @throws Exception
     */
    public void setCandidate(String host, int port, List<String> candidateHosts) throws Exception {
        logger.debug("set node status on : " + host + ":" + port);
        String candidatePath = zkPath + ApiCenter.makeZNodePath(host, port + "") + ConstUtils.ZK_CANDIDATE_PATH;
        Meta.Candidate candidate = new Meta.Candidate();
        candidate.setHost(candidateHosts);

        logger.debug("candidate : " + candidate);

        // set offline
        setOffline(host, port);

        if (client.checkExists().forPath(candidatePath) == null) {
            client.create().forPath(candidatePath, Meta.Candidate.marshalJson(candidate));
        } else {
            client.setData().forPath(candidatePath, Meta.Candidate.marshalJson(candidate));
        }

        setOnline(host, port);
    }

    /**
     * 给当前节点设置leader
     *
     * @param host
     * @param port
     * @param leader
     * @throws Exception
     */
    public void setLeader(String host, int port, String leader) throws Exception {
        logger.debug("set zkLeader " + leader + " for " + host);
        String leaderPath = zkPath +
                ApiCenter.makeZNodePath(host, port + "") + ConstUtils.ZK_LEADER_PATH;

        if (logger.isDebugEnabled()) {
            logger.debug(host + ":" + port + " set leader " + leader);
        }

        // set offline
        setOffline(host, port);

        if (client.checkExists().forPath(leaderPath) == null) {
            client.create().forPath(leaderPath, leader.getBytes());
        } else {
            client.setData().forPath(leaderPath, leader.getBytes());
        }

        setOnline(host, port);
    }

    /**
     * 增加终止节点
     * <p>
     * 第一步: 添加终止节点
     * 第二步: set offline
     * 第三步: wait for ephemeral-node disappear
     * 第四步: set online to start dump data again
     * </p>
     *
     * @param host
     * @param port
     * @param terminal
     */
    public void setTerminal(String host, int port, Meta.Terminal terminal) throws Exception {
        // 添加终止节点
        String terminalPath = zkPath +
                ApiCenter.makeZNodePath(host, port + "") + ConstUtils.ZK_TERMINAL_PATH;

        logger.debug("candidate : " + terminal.toString());

        // set offline
        setOffline(host, port);

        if (client.checkExists().forPath(terminalPath) == null) {
            client.create().forPath(terminalPath, Meta.Terminal.marshalJson(terminal));
        } else {
            logger.warn("terminal node exists for " + terminalPath + " then update to new data");
            client.setData().forPath(terminalPath, Meta.Terminal.marshalJson(terminal));
        }

        setOnline(host, port);
    }

    /**
     * 删除的节点的状态必须是offline 否则不能删除
     *
     * @param host
     * @param port
     * @throws Exception
     */
    public void remove(String host, int port) throws Exception {
        logger.warn("delete node host=" + host + ", port=" + port + " recursively");

        // instance path
        String ip = zkPath + ApiCenter.makeZNodePath(host, port + "");

        byte[] dbBts = client.getData().forPath(ip);
        Meta.DbInfo db = Meta.DbInfo.unmarshalJson(dbBts);
        if (db.getState() == Meta.NodeState.ONLINE) {
            throw new Exception("node 节点信息状态为 online 不能直接删除");
        }

        try {
            // 路径存在 则直接删除
            client.delete().deletingChildrenIfNeeded().forPath(ip);
        } catch (Exception exp) {
            logger.error(exp);
        }
    }

    /**
     * update admin node if exist or create admin node if not exist
     * @param admin
     */
    public void upsertAdminNode(Meta.Admin admin) throws Exception {
        String p = zkPath.substring(zkPath.indexOf("/")) + ConstUtils.ZK_ADMIN_PATH;
        if (nodeExist(p)) {
            // admin node exist then
            client.create().forPath(p, Meta.Admin.marshalJson(admin));
            return;
        }

        client.setData().forPath(p, Meta.Admin.marshalJson(admin));
    }
}
