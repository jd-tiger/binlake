package com.jd.binlog.zk;

import com.jd.binlog.alarm.AlarmUtils;
import com.jd.binlog.alarm.utils.JDMailParas;
import com.jd.binlog.alarm.utils.JDPhoneParas;
import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.config.bean.ServerConfig;
import com.jd.binlog.config.bean.ZKConfig;
import com.jd.binlog.dbsync.LogPosition;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.http.HttpServiceClient;
import com.jd.binlog.inter.alarm.IAlarm;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.inter.work.IWorkInitializer;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.meta.Http;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.util.ConstUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryNTimes;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.jd.binlog.meta.Meta.NodeState.OFFLINE;
import static com.jd.binlog.meta.Meta.NodeState.ONLINE;
import static com.jd.binlog.util.GTIDUtils.compare;

/**
 * Created by ninet on 16-12-27.
 * <p>`
 * <p>
 * ZkLeader Selector to take leader ship and hold it
 */
public class ZkLeaderSelector extends LeaderSelectorListenerAdapter implements ILeaderSelector {
    // configuration path in zookeeper
    private String path;
    private String binlogInfoPath;
    private String counterPath;
    private String terminalPath;
    private String candidatePath;
    private String leaderPath;
    private String errorPath;
    private String alarmPath;
    private String key;
    private MetaInfo metaInfo;

    // frequently used vars
    private String host;
    private ZKConfig zkConf;
    private ServerConfig serverConf;
    private HttpConfig httpConf;

    // interface
    private volatile IBinlogWorker work;
    private IWorkInitializer workIni; // 初始化work 工具

    // zookeeper client
    private CuratorFramework client;
    private LeaderSelector leaderSelector;

    // previous binlog when
    private volatile long preWhen;

    // update offset
    private Meta.BinlogInfo binlogInfo;

    // update offset lock
    private Lock lock = new ReentrantLock();

    public ZkLeaderSelector(String path,
                            String key,
                            ZKConfig zkConfig,
                            ServerConfig sc,
                            HttpConfig hc,
                            IWorkInitializer workIni) {
        this.workIni = workIni;

        this.client = CuratorFrameworkFactory.newClient(
                zkConfig.getServers(),
                new RetryNTimes(zkConfig.getRetryTimes(), zkConfig.getSleepMsBetweenRetries())
        );
        this.client.start();

        this.leaderSelector = new LeaderSelector(client, path, this);
        this.leaderSelector.autoRequeue();

        this.serverConf = sc;
        this.httpConf = hc;
        this.path = path;
        this.key = key;
        this.host = sc.getHost();

        this.zkConf = zkConfig;

        this.binlogInfoPath = path + ConstUtils.ZK_DYNAMIC_PATH;
        this.counterPath = path + ConstUtils.ZK_COUNTER_PATH;
        this.terminalPath = path + ConstUtils.ZK_TERMINAL_PATH;
        this.candidatePath = path + ConstUtils.ZK_CANDIDATE_PATH;
        this.leaderPath = path + ConstUtils.ZK_LEADER_PATH;
        this.errorPath = path + ConstUtils.ZK_ERROR_PATH;
        this.alarmPath = path + ConstUtils.ZK_ALARM_PATH;
    }

    public void startSelector() {
        this.leaderSelector.start();
    }

    public synchronized void close() {
        LogUtils.debug.debug("close selector");

        /**
         * 关闭首先关闭dump服务
         */
        IBinlogWorker work = this.work;

        if (work != null) {
            work.close();
        }

        LeaderSelector ls = this.leaderSelector;

        if (ls != null) {
            // 并且退出leader
            try {
                if (ls.hasLeadership()) {
                    // leader 前提下退出leader
                    abandonLeaderShip();
                }
            } catch (Throwable exp) {
                LogUtils.error.error("abandon leader ship error " + path, exp);
            }

            try {
                ls.close();
            } catch (Throwable exp) {
                LogUtils.error.error("close leader selector error " + path, exp);
                ZkConnRecycler.conns.offer(this);
                return;
            }
        }

        LogUtils.debug.debug("close CuratorFramework client");
        CuratorFramework client = this.client;

        if (client != null) {
            try {
                long sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
                LogUtils.info.info("session id 0x" + Long.toHexString(sessionId) + " have to close");
            } catch (Exception e) {
                LogUtils.error.error("client is not started " + path, e);
            }
            try {
                client.close();
            } catch (Throwable exp) {
                LogUtils.error.error("close curator framework error " + path, exp);
                ZkConnRecycler.conns.offer(this);
                return;
            }
        }

        reset();
        this.leaderSelector = null;
        this.zkConf = null;
        this.client = null;
        this.key = null;
    }

    @Override
    public void refreshLogPos() throws Exception {
        LogUtils.debug.debug("refreshLogPos");
        Lock lock = this.lock;
        if (lock != null && lock.tryLock()) { // 如果成功落锁
            try {
                IBinlogWorker worker = this.work;
                LogPosition logPos = null;
                MetaInfo metaInfo = this.metaInfo;
                if (worker != null && metaInfo != null && (logPos = worker.getLatestLogPosWithRm()) != null) {
                    // 比较当前binlog 取最新的位置
                    try {
                        if (binlogInfo.getWithGTID()) { // 如果开gtid 则比较gtid
                            usingGTID(logPos);
                        }
                    } catch (Throwable exp) {
                        // 如果有异常则
                        usingBinlogPos(logPos);
                    }

                    // 没有开gtid 则直接使用时间戳对比
                    if (!binlogInfo.getWithGTID()) {
                        usingBinlogPos(logPos);
                    }

                    LogUtils.debug.debug(binlogInfo); // 打印最新 位置

                    long currWhen = binlogInfo.getBinlogWhen();
                    if (preWhen < currWhen) {
                        updateBinlogInfo(Meta.BinlogInfo.marshalJson(this.binlogInfo));
                        preWhen = currWhen;
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @Override
    public Meta.BinlogInfo getLatestBinlogPos() {
        IBinlogWorker worker = this.work;
        LogPosition logPos = null;
        if (worker != null && (logPos = worker.getLatestLogPos()) != null) {
            Meta.BinlogInfo bi = new Meta.BinlogInfo().setBinlogFile(logPos.getFileName()).setBinlogPos(logPos.getPosition());
            if (logPos.hasGTID() && logPos.getGtidSets() != null) {
                bi.setExecutedGtidSets(logPos.getGtidSets());
            }
            return bi;
        }
        return null;
    }

    @Override
    public void refreshCandidate(Http.RefreshRequest req) {
        // refresh candidate and set leader and close worker if needed
        Lock lock = this.lock;
        if (lock != null && lock.tryLock()) {
            MetaInfo meta = this.metaInfo;
            if (meta == null) {
                return;
            }

            if (!StringUtils.startsWith(req.getKey(), meta.getHost())) {
                LogUtils.warn.warn("ephemeral node changed for different MySQL host with the previous is " + req.getKey() + ", while the current MySQL node is " + meta.getHost());
                return;
            }

            if (req.getLeaderVersion() < meta.getLeaderVersion()) {
                LogUtils.warn.warn("leader version changed for " + req.getKey() + " previous version " + req.getLeaderVersion() + " <  current version " + meta.getLeaderVersion());
                return;
            }

            try {
                Meta.Candidate can = new Meta.Candidate();
                can.setHost(req.getCandidate());

                Meta.Candidate oldCans = meta.getCandidate();

                boolean isEqual = false;
                // old candidates are equals with the new candidates then just abandon leader is ok
                if (oldCans != null && oldCans.getHost().size() == req.getCandidate().size() && req.getCandidate().containsAll(meta.getCandidate().getHost())) {
                    isEqual = true;
                }

                // if candidates are no equal
                if (isEqual) {
                    // update candidate
                    updateCandidate(Meta.Candidate.marshalJson(can), req.getLeader());

                    // abandon leader ship but not close leader selector
                    abandonLeaderShip();
                } else {
                    // start the leader selector and abandon self leader
                    Http.Response resp = new HttpServiceClient(req.getLeader(), serverConf.getHttpPort()).addLeaderSelector(req);
                    switch (resp) {
                        case SUCCESS:
                            // update candidate
                            updateCandidate(Meta.Candidate.marshalJson(can), req.getLeader());

                            // abandon leader and kill self
                            this.killWorkAndAbandonLeader();
                            break;
                        case FAILURE:
                            LogUtils.warn.warn("start the leader selector failure and give up the refresh candidate operation from agent");
                            break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 通过比较 binlog文件已经时间戳位置 来确定最新的binlog位置
     *
     * @param logPos
     */
    private void usingBinlogPos(LogPosition logPos) {
        // 时间是要大于
        if (logPos.getWhen() > binlogInfo.getBinlogWhen()) {
            binlogInfo.setExecutedGtidSets(logPos.getGtidSets())
                    .setBinlogFile(logPos.getFileName())
                    .setBinlogPos(logPos.getPosition())
                    .setBinlogWhen(logPos.getWhen());
        }

        // 时间要相等
        if (logPos.getWhen() == binlogInfo.getBinlogWhen()) {
            // 比较binlog文件
            // 如果binlog binlog文件序号一直增大
            if (logPos.getFileName().compareTo(binlogInfo.getBinlogFile()) > 0) {
                binlogInfo.setExecutedGtidSets(logPos.getGtidSets())
                        .setBinlogFile(logPos.getFileName())
                        .setBinlogPos(logPos.getPosition())
                        .setBinlogWhen(logPos.getWhen());
            }

            // binlog文件需要相等
            if (logPos.getFileName().equals(binlogInfo.getBinlogFile())) {
                if (logPos.getPosition() > binlogInfo.getBinlogPos()) {
                    binlogInfo.setExecutedGtidSets(logPos.getGtidSets())
                            .setBinlogFile(logPos.getFileName())
                            .setBinlogPos(logPos.getPosition())
                            .setBinlogWhen(logPos.getWhen());
                }
            }
        }
    }


    /**
     * 通过比较gtid 来确定最新的binlog 位置
     *
     * @param logPos
     * @throws Exception
     */
    private void usingGTID(LogPosition logPos) throws Exception {
        switch (compare(logPos.getGtidSets(), binlogInfo.getExecutedGtidSets())) {
            case 0:
                break;
            case 1:
                binlogInfo.setExecutedGtidSets(logPos.getGtidSets())
                        .setBinlogFile(logPos.getFileName())
                        .setBinlogPos(logPos.getPosition())
                        .setBinlogWhen(logPos.getWhen());
                break;
            case -1:
                break;
        }
    }

    /**
     * reset binlog info and meta info and work
     * <p>
     * other stay the same
     */
    private void reset() {
        this.binlogInfo = null;
        this.metaInfo = null;
        this.work = null;
    }

    public void takeLeadership(CuratorFramework client) throws Exception {
        LogUtils.debug.debug("takeLeadership");
        // 每次进入leader 清空上一次leader遗留下的位置信息
        reset();
        MetaInfo metaInfo = getMetaInfoFromZK(client);
        metaInfo.addVersion();
        int dumpLatch = serverConf.getDumpLatch();

        LogUtils.info.info("take leader for host " + metaInfo.getHost() + ":" + metaInfo.getPort());

        if (client.checkExists().forPath(terminalPath) != null && !checkTerminal(metaInfo, client)) {
            return;
        }

        if (metaInfo.getRetryTimes() > dumpLatch ||
                metaInfo.getDbInfo().getState().equals(OFFLINE)) {
            LogUtils.warn.warn("retry times " + metaInfo.getRetryTimes() + " exceed latch " + dumpLatch + " or state offline");
            close();
            return;
        }

        if (client.checkExists().forPath(candidatePath) != null) {
            setCandidate(metaInfo, client);
        }

        if (client.checkExists().forPath(leaderPath) != null && !checkLeader(metaInfo, client)) {
            return;
        }

        waitSeconds(metaInfo.getRetryTimes());
        String preLeader = metaInfo.getBinlogInfo().getLeader();

        // not the local host as the pre-leader
        if (!StringUtils.equals("", preLeader) && !host.equals(preLeader) && killPreviousLeader(preLeader, metaInfo)) {
            return; // keep on killing
        }

        IBinlogWorker work = null;
        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug(metaInfo.getHost() + " host before:" + metaInfo.getInstanceIp());
        }

        try {
            this.metaInfo = metaInfo;
            work = workIni.initWork(metaInfo, preLeader, this);

            updateBinlogInfo(Meta.BinlogInfo.marshalJson(metaInfo.getBinlogInfo()));

            removeLeaderPath();
        } catch (BinlogException exp) {
            LogUtils.error.error("init work failure " + metaInfo.getHost(), exp);

            byte[] errMsg = exp.message(metaInfo.getBinlogInfo().getLeader(), host, metaInfo.getHost());
            Meta.Error err = new Meta.Error().
                    setCode(exp.getErrorCode().errorCode).
                    setMsg(errMsg);

            LogUtils.warn.warn("add session retry times");

            switch (exp.getErrorCode().according()) {
                case Retry:
                    metaInfo.addSessionRetryTimes();
                    metaInfo.setError(err);
                    // retry without no retry
                    AlarmUtils.mail(metaInfo.getRetryTimes(),
                            metaInfo.getAlarm().getRetry(),
                            JDMailParas.mailParas(
                                    MetaInfo.mailTo(metaInfo.getAlarm().getUsers()),
                                    String.format(IAlarm.MailSubTemplate, host, metaInfo.getHost()),
                                    exp.message(metaInfo.getBinlogInfo().getLeader(), host, metaInfo.getHost())));
                    break;
                case Stop:
                    metaInfo.fillRetryTimes();
                    metaInfo.setError(err);
                    // stop for distribution error 目前只发给管理员
                    AlarmUtils.phone(JDPhoneParas.phoneParas(
                            exp.message(metaInfo.getBinlogInfo().getLeader(), host, metaInfo.getHost())));
                    break;
            }
            metaInfo.resetLeaderVersion(); // reset version because addVersion was called
            updateCounter(metaInfo); // 增加retryTimes 计数器

            if (work != null) {
                work.handleError(exp);
            }
            return;
        }

        this.binlogInfo = metaInfo.getBinlogInfo();
        this.work = work; // 赋值 work

        long sleepTime = serverConf.getTimerPeriod();

        try {
            // work 没有关闭 并且 两个work 地址 必须一致
            while (this.work == work && !work.isClosed()) {
                refreshLogPos();
                checkCandidate();
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException e) {
            // InterruptedException that means all have closed because of the interrupt
            if (this.work == work) { // 只有可以关闭当前work
                work.close(); // 此时已经退出leader
            }
        } catch (Throwable exp) {
            // 已经更新不上去 所以直接close
            LogUtils.error.error("refresh log postion error", exp);
            // 更新zk位置异常 停止work 退出leader

            if (this.work == work) { // 只有可以关闭当前work
                work.close(); // 此时已经退出leader
            }
        }
    }

    /**
     * remove leader path
     *
     * @throws BinlogException
     */
    private void removeLeaderPath() throws BinlogException {
        CuratorFramework client = this.client;
        if (client == null) {
            return;
        }

        // remove leader path in zookeeper
        try {
            if (client.checkExists().forPath(leaderPath) != null) {
                client.delete().forPath(leaderPath);
            }
        } catch (Exception e) {
            throw new BinlogException(ErrorCode.WARN_ZK_DELETE_PATH, e, leaderPath);
        }
    }

    /**
     * check candidate if needed
     */
    private void checkCandidate() {
        LogUtils.debug.debug("check candidate ");

        MetaInfo meta = this.metaInfo;
        if (meta == null) {
            return;
        }

        Lock lock = this.lock;
        if (lock != null && lock.tryLock()) {
            try {
                // current
                List<String> curr = meta.getCandidate().getHost();

                // valid candidates
                List<String> vcs = new ArrayList<>(curr.size());

                // un-valid candidates
                List<String> uvcs = new ArrayList<>();

                for (String h : curr) {
                    if (this.host.equals(h)) {
                        // current host is always valid
                        vcs.add(h);
                        continue;
                    }

                    try {
                        Http.Response resp = new HttpServiceClient(h, serverConf.getHttpPort()).isAlive();
                        switch (resp) {
                            case FAILURE:
                                // server cannot connect
                                LogUtils.warn.warn("candidate is of no used " + h + " on " + meta.getHost());
                                uvcs.add(h);
                                break;
                            case SUCCESS:
                                vcs.add(h);
                                break;
                        }
                    } catch (Exception e) {
                        LogUtils.error.error(e);
                    }
                }

                if (vcs.size() == curr.size()) {
                    LogUtils.debug.debug("candidate is all alive");
                    return;
                }

                try {
                    for (String c : new HttpServiceClient("127.0.0.1", serverConf.getAgentPort()).candidates()) {
                        if (vcs.size() >= curr.size()) {
                            // number of candidate stay the same
                            break;
                        }

                        // not exist in valid candidates and not exist in un-valid candidates
                        if (!vcs.contains(c) && !uvcs.contains(c)) {
                            vcs.add(c);
                        }

                        Meta.Candidate can = new Meta.Candidate();
                        can.setHost(vcs);

                        // no need to specified leader
                        updateCandidate(Meta.Candidate.marshalJson(can), "");

                        meta.setCandidate(can);
                    }
                } catch (Exception e) {
                    LogUtils.error.error(e);
                } finally {
                    // clear list
                    curr.clear();
                    uvcs.clear();
                }
            } finally {
                lock.unlock();
            }
        }

    }

    /**
     * kill previous leader according to the @para{preLeader}
     *
     * @param preLeader
     * @param metaInfo
     * @return <p>
     * false: no need to kill anymore
     * </p>
     * <p>
     * true: keep on killing the previous
     * </p>
     */
    private boolean killPreviousLeader(String preLeader, MetaInfo metaInfo) throws Exception {
        /**
         * if kill @killLatch times all return without @SUCCESS or @FAILURE
         *
         * Reason: net broken, or service is not exist
         *
         * just take over the leader role
         */
        if (metaInfo.getKilledTimes() < serverConf.getKillLatch()) {
            // here is make sure that it invalidate
            try {
                String[] hostPort = preLeader.split(":");

                /**
                 * compatible with the former using version
                 */
                int port = serverConf.getHttpPort();

                if (hostPort.length == 2) {
                    port = Integer.parseInt(hostPort[1]);
                }

                HttpServiceClient client = new HttpServiceClient(hostPort[0], port);
                switch (client.kill(metaInfo.getLeaderVersion(), key)) {
                    case FAILURE:
                        LogUtils.warn.warn("kill work failure: means leader is changed");
                        return true; // keep on kill
                    case SUCCESS:
                        metaInfo.resetKillTimes();
                        return false; // stop kill
                }
            } catch (Throwable e) {
                LogUtils.error.error("rpc kill exception " + metaInfo.getKilledTimes(), e);
                metaInfo.addSessionKillTimes();
                metaInfo.resetLeaderVersion(); // reset version because addVersion was called
                updateCounter(metaInfo);
                return true;
            }
        }
        return false; // stop kill
    }

    /**
     * check leader whether is the right host for leader
     *
     * @param metaInfo
     * @param client
     * @return <p>
     * false : means to stop dump
     * </p>
     * <p>
     * true: means to keep dump
     * </p>
     * @throws Exception
     */
    private boolean checkLeader(MetaInfo metaInfo, CuratorFramework client) throws Exception {
        String leader = new String(client.getData().forPath(leaderPath));

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug(metaInfo.getHost() + " new leader " + leader);
        }

        // 设置leader 只能设置一个值
        if (!host.equalsIgnoreCase(leader)) {
            LogUtils.warn.warn("host : " + host + " not equals to leader " + leader);
            return false;
        }

        return true;
    }

    /**
     * set candidate for meta
     *
     * @param metaInfo
     * @param client
     * @throws Exception
     */
    private void setCandidate(MetaInfo metaInfo, CuratorFramework client) throws Exception {
        byte[] cbs = client.getData().forPath(candidatePath);
        Meta.Candidate candidate = Meta.Candidate.unmarshalJson(cbs);
        metaInfo.setCandidate(candidate);

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug(metaInfo.getHost() + ":" + metaInfo.getPort() + ",candidate:" + candidate);
        }
    }

    private void waitSeconds(long retryTimes) throws InterruptedException {
        if (retryTimes == 0) {
            return;
        }
        Thread.sleep(ConstUtils.MaxWait(((long) Math.pow(2, retryTimes)) * 1000));
    }

    private MetaInfo getMetaInfoFromZK(CuratorFramework c) throws Exception {
        byte[] dbInfoByte = c.getData().forPath(path);
        byte[] binLogByte = c.getData().forPath(binlogInfoPath);
        byte[] counterBytes = c.getData().forPath(counterPath);

        Meta.DbInfo dbInfo = Meta.DbInfo.unmarshalJson(dbInfoByte);
        Meta.BinlogInfo binlogInfo = Meta.BinlogInfo.unmarshalJson(binLogByte);
        Meta.Counter counter = Meta.Counter.unmarshalJson(counterBytes);

        /**
         * here must compatible with previous version 4.0
         */
        Meta.Alarm alarm = null;
        if (c.checkExists().forPath(alarmPath) != null) {
            byte[] alarmBytes = c.getData().forPath(alarmPath);
            alarm = Meta.Alarm.unmarshalJson(alarmBytes);
        }

        return new MetaInfo(dbInfo, binlogInfo, counter, alarm == null ? Meta.Alarm.defalut() : alarm);
    }

    public void stateChanged(CuratorFramework client, ConnectionState newState) {
        try {
            LogUtils.warn.warn(client.getData().toString() + " state change to " + newState);
        } catch (Throwable exp) {
            // catch any null pointer exception
        }

        if (newState == ConnectionState.SUSPENDED || newState == ConnectionState.LOST) {
            try {
                refreshLogPos(); // 链接状态变更 则需要将最新的binlog offset 刷新到 zk
            } catch (Exception e) {
                // 更新binlog 位置异常
                LogUtils.error.error("state change to " + newState + " refresh log position error", e);
            }
            killWorkAndAbandonLeader();
        }
    }

    /**
     * update binlog info when
     * <p/>
     * "binlog file change"
     * "binlog position change"
     * etc.
     *
     * @param binlogBytes
     * @throws Exception
     */
    public void updateBinlogInfo(byte[] binlogBytes) throws BinlogException {
        LogUtils.debug.debug("updateBinlogInfo");
        CuratorFramework client = this.client;

        if (client != null) {
            try {
                client.setData().forPath(binlogInfoPath, binlogBytes);
            } catch (Exception e) {
                throw new BinlogException(ErrorCode.WARN_ZK_UPDATE_PATH, e, binlogInfoPath);
            }
        }
    }

    /**
     * update candidates
     *
     * @param cans
     */
    private void updateCandidate(byte[] cans, String leader) throws Exception {
        LogUtils.debug.debug("updateCandidate");
        CuratorFramework client = this.client;

        if (client != null) {
            CuratorTransaction trx = client.inTransaction();
            CuratorTransactionFinal ctf = trx.setData().forPath(candidatePath, cans).and();
            if (!StringUtils.equals("", leader)) {
                ctf = ctf.setData().forPath(leaderPath, leader.getBytes()).and();
            }
            // commit
            ctf.commit();
        }
    }

    /**
     * only startSelector and over can update the counter
     *
     * @param metaInfo
     * @throws Exception
     */
    public void updateCounter(MetaInfo metaInfo) throws Exception {
        LogUtils.debug.debug("updateCounter");
        metaInfo.refreshCounter();
        CuratorFramework client = this.client;

        if (client != null) {
            CuratorTransaction trx = client.inTransaction();
            CuratorTransactionFinal trf = trx.setData().forPath(counterPath, Meta.Counter.marshalJson(metaInfo.getCounter())).and();

            Meta.Error err = null;
            if ((err = metaInfo.getError()) != null) {
                trf = trf.setData().forPath(errorPath, Meta.Error.marshalJson(err)).and();
            }
            trf.commit();
        }
    }

    // kill dump connection and abandon leader
    public void killWorkAndAbandonLeader() {
        IBinlogWorker work = this.work;

        if (work != null && work.close()) {
            abandonLeaderShip();
        }
    }

    /**
     * 放弃leader 表明必须得停止dump
     * <p>
     * 停止dump 由外部调用 不是在这里调用 这里只是去除悬挂引用
     * <p>
     * 但是不关闭leader selector
     */
    public void abandonLeaderShip() {
        LogUtils.debug.debug("abandonLeaderShip");
        LeaderSelector leaderSelector = this.leaderSelector;

        if (leaderSelector != null) {
            leaderSelector.interruptLeadership();
        }
        this.work = null;
        this.metaInfo = null;
    }

    /**
     * @param terminal
     * @param metaInfo
     * @throws BinlogException
     */
    public void updateZNodesState(Meta.Terminal terminal, MetaInfo metaInfo) throws BinlogException {
        CuratorFramework client = this.client;
        List<String> newHosts = terminal.getNewHost();

        for (String host : newHosts) {
            LogUtils.info.info("start handle new host in the terminal node: " + host);
            String path = zkConf.getMetaPath() + File.separator + host;
            Meta.DbInfo dbInfo = null;
            //Meta.DbInfo.Builder builder = null;

            try {
                dbInfo = Meta.DbInfo.unmarshalJson(client.getData().forPath(path));
                //builder = dbInfo.toBuilder();

                switch (dbInfo.getState()) {
                    case ONLINE:
                        LogUtils.info.info("new host : " + host + " state is online already");
                        break;
                    case OFFLINE:
                        LogUtils.info.info("new host : " + host + " state is offline, need set online");

                        dbInfo.setState(ONLINE);
                        client.setData().forPath(path, Meta.DbInfo.marshalJson(dbInfo));

                        LogUtils.info.info("set new host : " + host + " online success!");
                        break;
                    default:
                        LogUtils.warn.warn("new host : " + host + " state is UNRECOGNIZED");
                        break;
                }
            } catch (Exception e) {
                LogUtils.error.error("set new host : " + host + " online failure!", e);
                BinlogException exp = new BinlogException(ErrorCode.WARN_ZK_START_NEWHOST, e, "");
                AlarmUtils.mail(0, 1, JDMailParas.mailParas(
                        MetaInfo.mailTo(metaInfo.getAlarm().getUsers()),
                        String.format(IAlarm.MailSubTemplate, this.host, host),
                        exp.message(this.host, host))
                );
            }
        }

        LogUtils.info.info("start set meta db offline....");
        Meta.DbInfo dbInfo = null;
        try {
            LogUtils.info.info("meta : " + metaInfo);
            dbInfo = metaInfo.getDbInfo();
            LogUtils.info.info("meta db state : " + dbInfo.getState());

            switch (dbInfo.getState()) {
                case ONLINE:
                    LogUtils.info.info("old host : " + metaInfo.getHost() + " state is online, need set offline");
                    dbInfo.setState(OFFLINE);
                    break;
                case OFFLINE:
                    LogUtils.info.info("old host : " + metaInfo.getHost() + " state is offline already");
                    break;
                default:
                    LogUtils.warn.warn("old host : " + metaInfo.getHost() + " state is UNRECOGNIZED");
                    break;
            }
            client.setData().forPath(path, Meta.DbInfo.marshalJson(dbInfo));
            LogUtils.info.info("set old host : " + metaInfo.getHost() + " offline success!");

            // 终止节点生效 需要删除老节点
            ZkPathRemover.paths.offer(path); // 后台监听到会自动从 lsm当中移除

            LogUtils.info.info("set old host : " + metaInfo.getHost() + " delete success!");
        } catch (Exception e) {
            LogUtils.error.error("set old host : " + metaInfo.getHost() + " offline failure!", e);
        }
    }

    public MetaInfo getMetaInfo() {
        return metaInfo;
    }

    public IBinlogWorker getWork() {
        return work;
    }

    public LeaderSelector getLeaderSelector() {
        return leaderSelector;
    }

    public CuratorFramework getClient() {
        return client;
    }

    /**
     * set terminal node to metaInfo
     *
     * @param metaInfo
     * @param client
     * @return false : mean to stop dump <p>
     * true : mean to keep dump <p>
     * @throws Exception
     */
    public boolean checkTerminal(MetaInfo metaInfo, CuratorFramework client) throws Exception {
        Meta.Terminal terminal = Meta.Terminal.unmarshalJson(client.getData().forPath(terminalPath));
        Meta.BinlogInfo binlogInfo = metaInfo.getBinlogInfo();
        if (terminal != null) {
            String current = "current gtid " + binlogInfo.getExecutedGtidSets() + "," + binlogInfo.getBinlogFile() + ":" + binlogInfo.getBinlogPos();
            String term = "terminal gtid " + terminal.getGtid() + "," + terminal.getBinlogFile() + ":" + terminal.getBinlogPos();
            try {
                // 比较gtid
                if (binlogInfo.getWithGTID() &&
                        compare(binlogInfo.getExecutedGtidSets(), terminal.getGtid()) >= 0) {
                    LogUtils.warn.warn("current gtid " + binlogInfo.getExecutedGtidSets() + ", terminal gtid " + terminal.getGtid());
                    updateZNodesState(terminal, metaInfo);
                    return false;
                }

                // 比较binlog 文件
                if (binlogInfo.getBinlogFile().compareTo(terminal.getBinlogFile()) > 0) {
                    LogUtils.warn.warn("current binlog file " + binlogInfo.getBinlogFile() + ", terminal binlog file " + terminal.getBinlogFile());
                    updateZNodesState(terminal, metaInfo);
                    return false;
                }

                // 比较binlog 文件偏移量
                if (binlogInfo.getBinlogFile().equalsIgnoreCase(terminal.getBinlogFile()) &&
                        binlogInfo.getBinlogPos() >= terminal.getBinlogPos()) {
                    LogUtils.warn.warn("binlog file of the same current binlog position " + binlogInfo.getBinlogPos()
                            + ", terminal binlog position " + terminal.getBinlogPos());
                    updateZNodesState(terminal, metaInfo);
                    return false;
                }
            } catch (Exception e) {
                LogUtils.error.error(current + "\n" + term, e);
            }

            LogUtils.info.info(metaInfo.getHost() + "\n" + current + "\n" + term);
            metaInfo.setTerminal(terminal);
        }
        return true;
    }
}
