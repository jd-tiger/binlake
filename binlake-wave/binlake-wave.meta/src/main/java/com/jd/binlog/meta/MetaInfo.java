package com.jd.binlog.meta;

import com.jd.binlog.util.LogUtils;

import java.util.List;

/**
 * Created by pengan on 16-12-15.
 */
public class MetaInfo {
    private String host;
    private volatile String instanceIp;
    private volatile int killedTimes;
    private volatile int retryTimes;
    private volatile long leaderVersion;
    private volatile String leader;
    private volatile String preLeader = "";

    /**
     * including db info and binlog Info combine the metaInfo
     */
    private Meta.DbInfo dbInfo;
    private Meta.BinlogInfo binlogInfo;
    private Meta.Counter counter;
    private Meta.Terminal terminal;
    private Meta.Candidate candidate;
    private Meta.Error error;
    private Meta.Alarm alarm;

    public MetaInfo(Meta.DbInfo dbInfo, Meta.BinlogInfo binlogInfo) {
        this.dbInfo = dbInfo;
        this.binlogInfo = binlogInfo;
        this.leaderVersion = binlogInfo.getLeaderVersion();
        this.leader = binlogInfo.getLeader();
        this.instanceIp = binlogInfo.getInstanceIp();
    }

    public MetaInfo(Meta.DbInfo dbInfo, Meta.BinlogInfo binlogInfo, Meta.Counter counter, Meta.Alarm alarm) {
        this(dbInfo, binlogInfo, counter);
        this.alarm = alarm;
    }

    public MetaInfo(Meta.DbInfo dbInfo, Meta.BinlogInfo binlogInfo, Meta.Counter counter) {
        this.dbInfo = dbInfo;
        this.binlogInfo = binlogInfo;
        this.counter = counter;
        this.leaderVersion = binlogInfo.getLeaderVersion();
        this.leader = binlogInfo.getLeader();
        this.instanceIp = binlogInfo.getInstanceIp();
        this.retryTimes = counter.getRetryTimes();
        this.killedTimes = counter.getKillTimes();
        this.preLeader = binlogInfo.getPreLeader();
    }

    public MetaInfo(Meta.DbInfo dbInfo, Meta.BinlogInfo binlogInfo, Meta.Counter counter, String host) {
        this(dbInfo, binlogInfo, counter);
        this.host = host;
    }

    /**
     * 增加 dump终止节点
     *
     * @param dbInfo
     * @param binlogInfo
     * @param counter
     * @param terminal
     */
    public MetaInfo(Meta.DbInfo dbInfo, Meta.BinlogInfo binlogInfo, Meta.Counter counter, Meta.Terminal terminal) {
        this(dbInfo, binlogInfo, counter);
        this.terminal = terminal;
    }

    public Meta.Terminal getTerminal() {
        return terminal;
    }

    public void setTerminal(Meta.Terminal terminal) {
        this.terminal = terminal;
    }

    public void setError(Meta.Error error) {
        this.error = error;
    }

    public int getPort() {
        return dbInfo.getPort();
    }

    public long getBinlogPosition() {
        return binlogInfo.getBinlogPos();
    }

    public long getSlaveId() {
        return dbInfo.getSlaveId();
    }

    public String getHost() {
        return dbInfo.getHost();
    }

    public String getUser() {
        return dbInfo.getUser();
    }

    public String getPassword() {
        return dbInfo.getPassword();
    }

    public String getBinlogFileName() {
        return binlogInfo.getBinlogFile();
    }

    public Meta.DbInfo getDbInfo() {
        return dbInfo;
    }

    public Meta.BinlogInfo getBinlogInfo() {
        return binlogInfo;
    }

    public Meta.Error getError() {
        return error;
    }

    public Meta.Alarm getAlarm() {
        return alarm;
    }

    public void setBinlogInfo(Meta.BinlogInfo binlogInfo) {
        this.binlogInfo = binlogInfo;
    }

    public void setDbInfo(Meta.DbInfo dbInfo) {
        this.dbInfo = dbInfo;
    }

    public int getRetryTimes() {
        return counter.getRetryTimes();
    }

    public long getKilledTimes() {
        return counter.getKillTimes();
    }


    public void resetKillTimes() {
        this.killedTimes = 0;
    }

    public String getInstance() {
        return getHost() + ":" + getPort();
    }

    public void setCandidate(Meta.Candidate candidate) {
        this.candidate = candidate;
    }

    public Meta.Candidate getCandidate() {
        return candidate;
    }

    /**
     * calling before updating to zk
     */
    public void refreshBinlogInfo() {
        LogUtils.debug.debug("refreshCounter");
        binlogInfo = binlogInfo.setLeaderVersion(leaderVersion)
                .setInstanceIp(instanceIp)
                .setPreLeader(preLeader)
                .setLeader(leader);

    }

    public Meta.Counter getCounter() {
        return counter;
    }

    public void refreshCounter() {
        counter = counter.setRetryTimes(retryTimes).setKillTimes(killedTimes);
    }

    public void addSessionRetryTimes() {
        LogUtils.info.info(host + " addSessionRetryTimes " + retryTimes);
        retryTimes++;
    }

    public void fillRetryTimes() {
        LogUtils.info.info(host + " addSessionRetryTimes " + retryTimes);
        retryTimes += 100;
    }

    public void addSessionKillTimes() {
        LogUtils.debug.debug("addSessionKillTimes");
        killedTimes++;
    }

    public void addVersion() {
        LogUtils.debug.debug("addVersion");
        leaderVersion++;
    }

    public void resetLeaderVersion() {
        LogUtils.debug.debug("decreaseVersion");
        leaderVersion = binlogInfo.getLeaderVersion();
    }

    public void setLeader(String leader) {
        LogUtils.debug.debug("setLeader");
        this.leader = leader;
    }

    /**
     * mail to users
     *
     * @param users
     * @return
     */
    public static String[] mailTo(List<Meta.User> users) {
        String[] to = new String[users.size()];

        for (int i = 0; i < to.length; i++) {
            to[i] = users.get(i).getEmail();
        }

        return to;
    }


    /***
     * phone to users
     *
     * @param users
     * @return
     */
    public static String[] phoneTo(List<Meta.User> users) {
        String[] to = new String[users.size()];

        for (int i = 0; i < to.length; i++) {
            to[i] = users.get(i).getPhone();
        }

        return to;
    }


    public void setPreLeader(String preLeader) {
        this.preLeader = preLeader;
    }

    public void setInstanceIp(String instanceIp) {
        this.instanceIp = instanceIp;
    }

    public String getInstanceIp() {
        return instanceIp;
    }

    public long getLeaderVersion() {
        return this.binlogInfo.getLeaderVersion();
    }

    public String getKey() {
        StringBuilder key = new StringBuilder();
        key.append(getSlaveId()).append(getHost()).append(getPort()).append(getUser());
        return key.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MetaInfo) {
            MetaInfo mi = (MetaInfo) obj;
            return mi.getSlaveId() == this.getSlaveId() &&
                    mi.getPort() == this.getPort() &&
                    mi.getHost().equals(this.getHost()) &&
                    mi.getUser().equals(this.getUser()) &&
                    mi.getPassword().equals(this.getPassword());
        }
        return false;
    }

    @Override
    public String toString() {
        return "MetaInfo{" +
                "dbInfo=" + dbInfo +
                ", binlogInfo=" + binlogInfo +
                ", counter=" + counter +
                '}';
    }
}
