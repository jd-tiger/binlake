package com.jd.binlog.mysql;


import com.jd.binlog.util.BufferUtils;

/**
 * Created by pengan on 16-9-29.
 */
public class OKPacket extends MySQLPacket {
    public static final byte[] OK_PACKET = new byte[]{7, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0};
    public static final int FIELD_COUNT = 0x00;

    public byte fieldCount = FIELD_COUNT;
    public long affectedRows;
    public long insertId;
    public int serverStatus;
    public int warningCount;
    public byte[] message;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        fieldCount = mm.read();
        affectedRows = mm.readLength();
        insertId = mm.readLength();
        serverStatus = mm.readUB2();
        warningCount = mm.readUB2();
        if (mm.hasRemaining()) {
            this.message = mm.readBytesWithLength();
        }
    }

    @Override
    public int calcPacketSize() {
        int i = 1;
        i += BufferUtils.getLength(affectedRows);
        i += BufferUtils.getLength(insertId);
        i += 4;
        if (message != null) {
            i += BufferUtils.getLength(message);
        }
        return i;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL OK Packet";
    }
}
