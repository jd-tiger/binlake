package com.jd.binlog.mysql;

/**
 * Created by pengan on 16-12-18.
 */
public class HeaderPacket extends MySQLPacket {
    public void read(byte[] data) {
        if (data == null || data.length != 4) {
            throw new IllegalArgumentException("invalid header data. It can't be null and the length must be 4 byte.");
        }
        packetLength = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8) | ((data[2] & 0xFF) << 16);
        packetId = data[3];
    }

    public byte[] getBytes() {
        byte[] data = new byte[4];
        data[0] = (byte) (packetLength & 0xFF);
        data[1] = (byte) (packetLength >>> 8);
        data[2] = (byte) (packetLength >>> 16);
        data[3] = packetId;
        return data;
    }

    public int calcPacketSize() {
        return 4;
    }

    protected String getPacketInfo() {
        return "MySQL Header Packet";
    }
}
