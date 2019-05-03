package com.jd.binlog.mysql;

import java.io.IOException;
import java.io.OutputStream;

import com.jd.binlog.util.BufferUtils;
import com.jd.binlog.util.StreamUtils;

/**
 * Created by pengan on 16-9-25.
 */
public class AuthPacket extends MySQLPacket {
    private static final byte[] FILLER = new byte[23];

    public long clientFlags;
    public long maxPacketSize;
    public int charsetIndex;
    public byte[] extra;// from FILLER(23)
    public String user;
    public byte[] password;
    public String database;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        clientFlags = mm.readUB4();
        maxPacketSize = mm.readUB4();
        charsetIndex = (mm.read() & 0xff);
        // readFromChannel extra
        int current = mm.position();
        int len = (int) mm.readLength();
        if (len > 0 && len < FILLER.length) {
            byte[] ab = new byte[len];
            System.arraycopy(mm.bytes(), mm.position(), ab, 0, len);
            this.extra = ab;
        }
        mm.position(current + FILLER.length);
        user = mm.readStringWithNull().toLowerCase();
        password = mm.readBytesWithLength();
        if (((clientFlags & Capabilities.CLIENT_CONNECT_WITH_DB) != 0) && mm.hasRemaining()) {
            database = mm.readStringWithNull();
        }
    }

    public void write(OutputStream out) throws IOException {
        StreamUtils.writeUB3(out, calcPacketSize());
        StreamUtils.write(out, packetId);
        StreamUtils.writeUB4(out, clientFlags);
        StreamUtils.writeUB4(out, maxPacketSize);
        StreamUtils.write(out, (byte) charsetIndex);
        out.write(FILLER);
        if (user == null) {
            StreamUtils.write(out, (byte) 0);
        } else {
            StreamUtils.writeWithNull(out, user.getBytes());
        }
        if (password == null) {
            StreamUtils.write(out, (byte) 0);
        } else {
            StreamUtils.writeWithLength(out, password);
        }
        if (database == null) {
            StreamUtils.write(out, (byte) 0);
        } else {
            StreamUtils.writeWithNull(out, database.getBytes());
        }
    }

    @Override
    public int calcPacketSize() {
        int size = 32;// 4+4+1+23;
        size += (user == null) ? 1 : user.length() + 1;
        size += (password == null) ? 1 : BufferUtils.getLength(password);
        size += (database == null) ? 1 : database.length() + 1;
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return null;
    }
}
