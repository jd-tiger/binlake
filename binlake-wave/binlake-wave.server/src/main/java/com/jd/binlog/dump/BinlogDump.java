package com.jd.binlog.dump;

import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.DumpPacket;
import com.jd.binlog.mysql.GTIDDumpPacket;

import java.io.IOException;

/**
 * Created on 18-5-15
 *
 * @author pengan
 */
public class BinlogDump {
    /**
     * send binlog dump command to new MySQL server
     *
     * @param connector
     */
    public static void sendDumpCommand(MySQLConnector connector,
                                       DumpType type, MetaInfo metaInfo) throws IOException {
        switch (type) {
            case COM_BINLOG_DUMP:
                DumpPacket dump = new DumpPacket();
                dump.packetId = 0x00;
                dump.binlogPos = metaInfo.getBinlogPosition();
                dump.slaveId = metaInfo.getSlaveId();
                dump.binlogFileName = metaInfo.getBinlogFileName();
                dump.write(connector.getChannel().socket().getOutputStream());
                break;
            case COM_BINLOG_DUMP_GTID:
                GTIDDumpPacket gtidDump = new GTIDDumpPacket();
                gtidDump.packetId = 0x00;
                gtidDump.executedGtidSet = metaInfo.getBinlogInfo().getExecutedGtidSets();
                gtidDump.slaveId = metaInfo.getSlaveId();
                gtidDump.write(connector.getChannel().socket().getOutputStream());
        }
        connector.getChannel().socket().getOutputStream().flush();
    }
}
