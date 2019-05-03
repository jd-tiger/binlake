package com.jd.binlog.mysql;

import com.jd.binlog.util.BufferUtils;
import com.jd.binlog.util.StreamUtils;
import com.jd.binlog.util.UUIDUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created by pengan on 17-1-12.
 * <p/>
 * 1              [1e] COM_BINLOG_DUMP_GTID
 * 2              flags
 * 4              server-id
 * 4              binlog-filename-len
 * string[len]    binlog-filename
 * 8              binlog-pos
 * if flags & BINLOG_THROUGH_GTID {
 * 4              data-size
 * string[len]    data
 * }
 * <p/>
 * example: e6954592-8dba-11e6-af0e-fa163e1cf111:1-5:11-18,
 * e6954592-8dba-11e6-af0e-fa163e1cf3f2:1-27
 */
public class GTIDDumpPacket extends MySQLPacket {
    private static final String GTID_SEPARATOR = ",";
    private static final String GTID_INTERVAL_SEPARATOR = ":";
    private static final String INTERVAL_SEPARATOR = "-";

    private static final int flags = 0;
    private static final byte[] binlogFileNameLen = {16, 00, 00, 00};
    private static final byte[] binlogFileName = {
            00, 00, 00, 00, 00, 00, 00, 00,
            00, 00, 00, 00, 00, 00, 00, 00};
    private static final byte[] binlogPos = {04, 00, 00, 00, 00, 00, 00, 00};

    public long slaveId;

    private long nSids;
    private long dataLen = 0;
    // public int sidsNum;
    public String executedGtidSet;


    public void write(OutputStream out) throws IOException {
        calDataLen();

        // write header
        StreamUtils.writeUB3(out, calcPacketSize());
        StreamUtils.write(out, packetId);

        // write command type
        StreamUtils.write(out, COM_BINLOG_DUMP_GTID);

        // write protocol
        StreamUtils.writeUB2(out, flags);
        StreamUtils.writeUB4(out, slaveId);
        StreamUtils.writeBytes(out, binlogFileNameLen);
        StreamUtils.writeBytes(out, binlogFileName);
        StreamUtils.writeBytes(out, binlogPos);

        // write data length
        StreamUtils.writeUB4(out, dataLen);


        // write data
        StreamUtils.writeLong(out, nSids);
        String[] gtidSplits = null;
        String[] interval = null;
        long start, end;
        for (String gtid : executedGtidSet.split(GTID_SEPARATOR)) {
            // index :0 is for uuid & index 1 is for interval
            gtidSplits = gtid.trim().split(GTID_INTERVAL_SEPARATOR);
            StreamUtils.writeBytes(out, UUIDUtils.UUIDToHexBytes(gtidSplits[0]));
            StreamUtils.writeLong(out, gtidSplits.length - 1);

            for (int index = 1; index <= gtidSplits.length - 1; index++) {
                interval = gtidSplits[index].split(INTERVAL_SEPARATOR);

                start = Long.parseLong(interval[0]);
                if (interval.length == 1) {
                    end = start;
                } else {
                    end = Long.parseLong(interval[1]);
                }
                StreamUtils.writeLong(out, start);
                StreamUtils.writeLong(out, end + 1L);
            }
        }
    }

    /**
     * calculate data length and just
     */
    private void calDataLen() {
        String[] gtids = executedGtidSet.split(GTID_SEPARATOR);
        nSids = gtids.length;

        // calculate data length
        dataLen = 8; // for n_sids 8 bytes
        for (String gtid : gtids) {
            dataLen += 16; // for uuid 16 bytes
            int nIntervals = gtid.trim().split(GTID_INTERVAL_SEPARATOR).length - 1;
            dataLen += 8; // for n_intervals 8 bytes
            dataLen += (nIntervals * 16); // for start & end 16 bytes
        }
    }

    @Override
    public int calcPacketSize() {
        return 1 + 2 + 4 + 4 + binlogFileName.length + 8 + 4 + (int) dataLen;
    }

    protected String getPacketInfo() {
        return "binlog gtid dump packet";
    }

    /**
     * lower is for test
     */
    public ByteBuffer write() {
        calDataLen();

        ByteBuffer buffer = ByteBuffer.allocate(1024);
        // header
        BufferUtils.writeUB3(buffer, calcPacketSize()); // packet length
        buffer.put(packetId);

        // command type
        buffer.put(COM_BINLOG_DUMP_GTID);

        // protocol
        BufferUtils.writeUB2(buffer, flags);
        BufferUtils.writeUB4(buffer, slaveId);
        BufferUtils.writeBytes(buffer, binlogFileNameLen);
        BufferUtils.writeBytes(buffer, binlogFileName);
        BufferUtils.writeBytes(buffer, binlogPos);

        // write data length
        BufferUtils.writeUB4(buffer, dataLen);

        // write data
        BufferUtils.writeLong(buffer, nSids);

        String[] gtidSplits = null;
        String[] interval = null;
        long start, end;
        for (String gtid : executedGtidSet.split(GTID_SEPARATOR)) {
            // index :0 is for uuid & index 1 is for interval
            gtidSplits = gtid.trim().split(GTID_INTERVAL_SEPARATOR);

            // write uuid hex bytes to buffer
            BufferUtils.writeBytes(buffer, UUIDUtils.UUIDToHexBytes(gtidSplits[0]));
            BufferUtils.writeLong(buffer, gtidSplits.length - 1);

            for (int index = 1; index <= gtidSplits.length - 1; index++) {
                interval = gtidSplits[index].split(INTERVAL_SEPARATOR);

                start = Long.parseLong(interval[0]);
                if (interval.length == 1) {
                    end = start;
                } else {
                    end = Long.parseLong(interval[1]);
                }
                BufferUtils.writeLong(buffer, start);
                BufferUtils.writeLong(buffer, end + 1L);
            }
        }
        return buffer;
    }
}
