package com.jd.binlog.conn;

import com.jd.binlog.dbsync.LogEvent;

/**
 * Created by pengan on 17-5-2.
 *
 * <p>
 *     执行MySQL 设置
 * </p>
 */
public class MySQLUpdates {
    private String slaveUuid;
    private static final String waitTimeout = "set wait_timeout=9999999";
    private static final String netWriteTimeout = "set net_write_timeout=1800";
    private static final String netReadTimeout = "set net_read_timeout=1800";
    private static final String names = "set names 'binary'";
    private static final String binlogCheckSum = "set @master_binlog_checksum='NONE'";
    private static final String slaveCapability =
            "SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'";

    public MySQLUpdates(String slaveUuid) {
        this.slaveUuid = "set @slave_uuid=\"" + slaveUuid + "\"";
    }

    public String getSlaveUuid() {
        return slaveUuid;
    }

    public String getWaitTimeout() {
        return waitTimeout;
    }

    public String getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public String getNetReadTimeout() {
        return netReadTimeout;
    }

    public String getNames() {
        return names;
    }

    public String getBinlogCheckSum() {
        return binlogCheckSum;
    }

    public String getSlaveCapability() {
        return slaveCapability;
    }


}
