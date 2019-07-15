package com.jd.binlog.domain;

import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.*;
import com.jd.binlog.util.ExecutorUtils;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.util.NetUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by pengan on 17-2-23.
 * <p>
 * mysql domain name switch to another ip
 * <p>
 */
public abstract class DomainSwitchTracker {
    protected final int DEFAULT_BINLOG_START_OFFSET = 4;

    static final ThreadPoolExecutor switchExecutors = ExecutorUtils.create("switch", 4);

    // transaction have to be in 20 seconds
    // if not using trackEvent

    /**
     * just using builder is ok
     */
    protected MetaInfo metaInfo;
    protected HttpConfig httpConf;

    protected long lastEventTime; // last binlog event time
    protected String executedGTIDSets; // executed gtid sets

    protected Meta.BinlogInfo binlogInfo;

    public DomainSwitchTracker(MetaInfo metaInfo, HttpConfig httpConf) {
        this.metaInfo = metaInfo;
        this.httpConf = httpConf;
        this.binlogInfo = metaInfo.getBinlogInfo();
        this.lastEventTime = this.binlogInfo.getBinlogWhen();
        this.executedGTIDSets = this.binlogInfo.getExecutedGtidSets();
    }

    /**
     * back binlogTrack
     * <p>
     * if any exception occur in fix binlog offset also will be met in session start
     * <p>
     * always get the nearest binlog event
     * <p>
     * consider the transaction event firstly
     *
     * @throws IOException domain changed but not find the right offset
     */
    public void binlogTrack() throws IOException {
        String ip = null;

        try {
            ip = resolveDomain(); // 解析域名
        } catch (Throwable exp) {
            // 域名解析异常
            LogUtils.error.error("resolve domain error " + metaInfo.getHost(), exp);
            return;
        }

        if (isDomainSwitch(ip)) {
            // TODO: 18-7-4  域名切换 是否需要直接确认binlog格式
            checkBinlogFormat();

            LogUtils.info.info("domain is switched");
            binlogInfo.setInstanceIp(ip);
            try {
                LogUtils.info.info("track transaction");
                trackTransaction();

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("transaction binlog info : " + binlogInfo);
                }

                // using the previous event for not to lose the correct binlog event in case of Exception
                metaInfo.setBinlogInfo(binlogInfo);
                metaInfo.setInstanceIp(ip);
                return;
            } catch (Throwable exp) {
                throw new IOException(exp.getMessage());
                /**
                 * get event to nearest timestamp is error because there is no tableMap
                 */
            }
        }
        metaInfo.setInstanceIp(ip);
    }

    /**
     * 检查 binlog 格式是否正确
     *
     * @return
     */
    protected void checkBinlogFormat() throws IOException {
        Meta.DbInfo dbInfo = metaInfo.getDbInfo();
        MySQLConnector connector = new MySQLConnector(dbInfo);
        try {
            connector.connect();
            MySQLExecutor exec = new MySQLExecutor(connector);
            ResultSetPacket rst = exec.query("SHOW VARIABLES LIKE \"%BINLOG_FORMAT%\"");
            if (rst.getFieldValues().size() > 1 && rst.getFieldValues().get(1).equalsIgnoreCase("ROW")) {
                return;
            }
            String message = "instance " + dbInfo.getHost() + ":" + dbInfo.getPort() + " binlog format is not 'ROW'";
            throw new IOException(message);
        } finally {
            try {
                connector.disconnect();
            } catch (IOException e) {
                // 释放链接
            }
        }
    }

    /**
     * is domain changed
     * <p>true  : domain is changed</p>
     * <p>false : domain is not changed</p>
     *
     * @return
     */
    private boolean isDomainSwitch(String ip) {
        if (StringUtils.equals(metaInfo.getBinlogInfo().getInstanceIp(), "")) {
            binlogInfo.setInstanceIp(ip);
            return false;
        }
        boolean equal = StringUtils.equalsIgnoreCase(ip, metaInfo.getBinlogInfo().getInstanceIp());

        if (equal) {// 如果ip 相同
            return false;
        }
        binlogInfo.setInstanceIp(ip);
        return true;
    }

    /**
     * 域名解析
     *
     * @return 返回ip
     */
    private String resolveDomain() {
        if (NetUtils.isIPv4Address(metaInfo.getHost())) {
            LogUtils.warn.warn("host : " + metaInfo.getHost() + " is ip");
            binlogInfo.setInstanceIp(metaInfo.getHost());
            return metaInfo.getHost();
        }

        InetSocketAddress isa = new InetSocketAddress(metaInfo.getHost(), metaInfo.getPort());
        String ip = isa.getAddress().getHostAddress();

        if (isa.getAddress() == null || StringUtils.equals(isa.getAddress().getHostAddress(), metaInfo.getHost())) {
            // domain have no resolution or resolve error
            throw new BinlogException(ErrorCode.WARN_DOMAIN_RESOLVE, new Exception(" host " + metaInfo.getHost() + " resolved error"), metaInfo.getInstanceIp());
        }
        return ip;
    }

    /**
     * get previous binlog file name with current index - 1
     *
     * @param binlogFile
     * @return
     */
    protected String getPreBinlogFile(String binlogFile) {
        int len = binlogFile.length();
        String prefix = binlogFile.substring(0, len - 6);
        String suffix = binlogFile.substring(len - 6, len);
        long index = Long.parseLong(suffix);
        return prefix + String.format("%06d", index - 1);
    }


    /**
     * send binlog dump command to new MySQL server
     *
     * @param binlogInfo
     * @param connector
     */
    protected void sendDumpCommand(long slaveId,
                                   Meta.BinlogInfo binlogInfo,
                                   MySQLConnector connector) throws IOException {
        DumpPacket dump = new DumpPacket();
        dump.packetId = 0x00;
        dump.binlogPos = DEFAULT_BINLOG_START_OFFSET;
        dump.slaveId = slaveId;
        dump.binlogFileName = binlogInfo.getBinlogFile();
        dump.write(connector.getChannel().socket().getOutputStream());
        connector.getChannel().socket().getOutputStream().flush();
    }

    /**
     * check binlog file whether exist on the binlog file and binlog offset
     *
     * @param binlogFile
     * @param connector
     * @return <p>true  : binlog file and binlog position are correct</p>
     * <p>false : binlog file not exist in MySQL server</p>
     * @throws IOException something wrong with the socket channel
     */
    protected boolean isBinlogExist(String binlogFile, MySQLConnector connector)
            throws IOException {
        MySQLExecutor executor = new MySQLExecutor(connector);
        String sql = "SHOW BINLOG EVENTS IN '" + binlogFile + "' FROM " + DEFAULT_BINLOG_START_OFFSET + " LIMIT 1";
        try {
            executor.query(sql);
        } catch (BinlogException exp) {
            // ERROR 1220 (HY000): Error when executing command SHOW BINLOG EVENTS: Could not find target log
            LogUtils.error.error("execute sql " + sql + " error", exp);
            return false;
        }
        return true;
    }

    /**
     * get new host binlog offset Using show master status
     *
     * @param metaInfo
     */
    protected void getNewBinlogOffset(MetaInfo metaInfo, MySQLConnector connector) throws IOException {
        MySQLExecutor executor = new MySQLExecutor(connector);
        ResultSetPacket rst = null;
        rst = executor.query("SHOW MASTER STATUS");

        String binlogFile = rst.getFieldValues().get(0); // binlog file
        long binlogPos = Long.parseLong(rst.getFieldValues().get(1)); // binlog position

        // return new binlog info
        binlogInfo.setBinlogFile(binlogFile).setBinlogPos(binlogPos);
    }

    /**
     * update MySQL connection properties
     *
     * @param connector
     * @throws IOException
     */
    protected void updateConnectionProps(MySQLConnector connector) throws IOException {
        MySQLExecutor exe = new MySQLExecutor(connector);
        exe.setConnCfg(metaInfo.getDbInfo().getSlaveUUID());
    }


    protected void registerSlave(MySQLConnector connector) throws IOException {
        LogUtils.info.info("register slave");
        RegisterSlavePacket rsp = new RegisterSlavePacket();
        rsp.slaveId = metaInfo.getSlaveId();
        rsp.write(connector.getChannel().socket().getOutputStream());
        HeaderPacket header = null;
        header = PacketUtils.readHeader(connector.getChannel(), DEFAULT_BINLOG_START_OFFSET);
        byte[] bytes = PacketUtils.readBytes(connector.getChannel(), header.packetLength);

        switch (bytes[0]) {
            case OKPacket.FIELD_COUNT:
                return;
            default:
                throw new IOException("register slave error");
        }
    }

    /**
     * 获取最近的binlog事件
     *
     * @return
     * @throws IOException
     */
    public abstract void trackEvent() throws IOException;

    /**
     * 获取最近的binlog begin事件
     *
     * @return
     * @throws IOException
     */
    public abstract void trackTransaction() throws IOException;
}
