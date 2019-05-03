package com.jd.binlog.mysql;

import com.jd.binlog.util.StreamUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by pengan on 16-12-16.
 */
public class DumpPacket extends MySQLPacket {
    public static final int BINLOG_SEND_ANNOTATE_ROWS_EVENT = 2;
    public static final int BINLOG_DUMP_NON_BLOCK = 1;
    // binlog pos : 4 bytes, binlog flags : 2 bytes, binlog file name[stringEof]
    private final byte command = COM_BINLOG_DUMP;
    public int binlogFlags = (0 | BINLOG_SEND_ANNOTATE_ROWS_EVENT);
    public long binlogPos;
    public long slaveId;
    public String binlogFileName;

    public void write(OutputStream out) throws IOException {
        StreamUtils.writeUB3(out, calcPacketSize());
        StreamUtils.write(out, packetId);
        StreamUtils.write(out, command);
        StreamUtils.writeUB4(out, binlogPos);
        StreamUtils.writeUB2(out, binlogFlags);
        StreamUtils.writeUB4(out, slaveId);
        StreamUtils.writeWithNull(out, binlogFileName.getBytes());
    }

    public int calcPacketSize() {
        return 1 + 4 + 2 + 4 + binlogFileName.length();
    }

    protected String getPacketInfo() {
        return "Binlog dump command";
    }
}
