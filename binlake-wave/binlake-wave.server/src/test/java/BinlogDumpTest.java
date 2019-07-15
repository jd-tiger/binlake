import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.LogContext;
import com.jd.binlog.dbsync.LogDecoder;
import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.event.RotateLogEvent;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.*;

import java.io.IOException;

/**
 * Created on 18-7-15
 *
 * @author pengan
 */
public class BinlogDumpTest {
    private static String Host;
    private static String BinlogFile;
    private static long BinlogPos;

    public static void main(String[] args) throws IOException {
        DumpThread dt1 = new DumpThread(5000, "757a2411-7429-4b9b-8c48-0f9c8u3b65a7");
        /*Host = args[0];
        BinlogFile = args[1];
        BinlogPos = Long.parseLong(args[2]);*/
        Host = "127.0.0.1";
        BinlogFile = "mysql-bin.000003";
        BinlogPos = 4;

        dt1.start();
    }

    static class DumpThread extends Thread {
        long slaveId;
        String uuid;

        public DumpThread(long slaveId, String uuid) {
            this.slaveId = slaveId;
            this.uuid = uuid;
        }

        @Override
        public void run() {
            try {
                dumpTest(slaveId, uuid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void dumpTest(long slaveId, String uuid) throws IOException {
        Meta.DbInfo dbInfo = new Meta.DbInfo().setHost(Host).setSlaveId(slaveId)
                .setPort(3306).setUser("root").setPassword("secret");

        Meta.BinlogInfo binlogInfo = new Meta.BinlogInfo().setBinlogFile(BinlogFile)
                .setBinlogPos(BinlogPos);

        MetaInfo metaInfo = new MetaInfo(dbInfo, binlogInfo, new Meta.Counter());
        MySQLConnector connector = new MySQLConnector(metaInfo.getDbInfo());

        LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        LogContext context = new LogContext();


        connector.handshake();
        MySQLExecutor executor = new MySQLExecutor(connector);
        executor.update("set @master_binlog_checksum='NONE'");
        executor.update("set @slave_uuid='" + uuid + "'");

        DumpPacket dump = new DumpPacket();
        dump.packetId = 0x00;
        dump.binlogPos = metaInfo.getBinlogPosition();
        dump.slaveId = metaInfo.getSlaveId();
        dump.binlogFileName = metaInfo.getBinlogFileName();

//        GTIDDumpPacket dump = new GTIDDumpPacket();
//        dump.packetId = 0x00;
//        dump.slaveId = metaInfo.getSlaveId();
//        dump.executedGtidSet = "52737a38-c4e5-4adc-9f0b-cb4ffda1cf8a:1-1000";
        dump.write(connector.getChannel().socket().getOutputStream());
        connector.getChannel().socket().getOutputStream().flush();

        HeaderPacket header = null;
        LogEvent logEvent = null;
        // each time just on one binlog file, what to find is the previous begin
        String binlogFile = "";
        while (true) {
            header = PacketUtils.readHeader(connector.getChannel(), 4);

            byte[] data = PacketUtils.readBytes(connector.getChannel(), header.packetLength);

            switch (data[0]) {
                case ErrorPacket.FIELD_COUNT:

                    ErrorPacket e = new ErrorPacket();
                    e.read(data);
                    System.err.println(e.errno);
                    System.err.println(new String(e.message));
                    return;
            }

            LogBuffer buffer = new LogBuffer(data, 1, data.length - 1);
            logEvent = decoder.decode(buffer, context);
            if (logEvent != null) {
                switch (logEvent.getHeader().getType()) {
                    case LogEvent.ROTATE_EVENT:
                        RotateLogEvent re = (RotateLogEvent) logEvent;
                        binlogFile = re.getFilename();
                        break;
                }
                System.err.println("binlog file " + binlogFile + ", binlog position " + logEvent.getHeader().getLogPos());
                System.err.println("binlog file " + binlogFile + ", event length " + logEvent.getHeader().getEventLen());
            }
        }

    }
}
