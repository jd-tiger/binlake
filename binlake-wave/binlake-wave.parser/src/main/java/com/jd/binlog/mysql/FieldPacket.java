package com.jd.binlog.mysql;


import com.jd.binlog.util.BufferUtils;

import java.nio.ByteBuffer;

/**
 * Created by pengan on 16-10-11.
 */
public class FieldPacket extends MySQLPacket {
    private static final byte[] DEFAULT_CATALOG = "def".getBytes();
    private static final byte[] FILLER = new byte[2];

    public byte[] catalog = DEFAULT_CATALOG;
    public byte[] db;
    public byte[] table;
    public byte[] orgTable;
    public byte[] name;
    public byte[] orgName;
    public int charsetIndex;
    public long length;
    public int type;
    public int flags;
    public byte decimals;
    public byte[] definition;

    /**
     * 把字节数组转变成FieldPacket
     */
    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        this.packetLength = mm.readUB3();
        this.packetId = mm.read();
        readBody(mm);
    }

    public void read(byte[] data, boolean noHead) {
        MySQLMessage mm = new MySQLMessage(data);
        readBody(mm);
    }


    @Override
    public int calcPacketSize() {
        int size = (catalog == null ? 1 : BufferUtils.getLength(catalog));
        size += (db == null ? 1 : BufferUtils.getLength(db));
        size += (table == null ? 1 : BufferUtils.getLength(table));
        size += (orgTable == null ? 1 : BufferUtils.getLength(orgTable));
        size += (name == null ? 1 : BufferUtils.getLength(name));
        size += (orgName == null ? 1 : BufferUtils.getLength(orgName));
        size += 13;// 1+2+4+1+2+1+2
        if (definition != null) {
            size += BufferUtils.getLength(definition);
        }
        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Field Packet";
    }

    private void readBody(MySQLMessage mm) {
        this.catalog = mm.readBytesWithLength();
        this.db = mm.readBytesWithLength();
        this.table = mm.readBytesWithLength();
        this.orgTable = mm.readBytesWithLength();
        this.name = mm.readBytesWithLength();
        this.orgName = mm.readBytesWithLength();
        mm.move(1);
        this.charsetIndex = mm.readUB2();
        this.length = mm.readUB4();
        this.type = mm.read() & 0xff;
        this.flags = mm.readUB2();
        this.decimals = mm.read();
        mm.move(FILLER.length);
        if (mm.hasRemaining()) {
            this.definition = mm.readBytesWithLength();
        }
    }

    private void writeBody(ByteBuffer buffer) {
        byte nullVal = 0;
        BufferUtils.writeWithLength(buffer, catalog, nullVal);
        BufferUtils.writeWithLength(buffer, db, nullVal);
        BufferUtils.writeWithLength(buffer, table, nullVal);
        BufferUtils.writeWithLength(buffer, orgTable, nullVal);
        BufferUtils.writeWithLength(buffer, name, nullVal);
        BufferUtils.writeWithLength(buffer, orgName, nullVal);
        buffer.put((byte) 0x0C);
        BufferUtils.writeUB2(buffer, charsetIndex);
        BufferUtils.writeUB4(buffer, length);
        buffer.put((byte) (type & 0xff));
        BufferUtils.writeUB2(buffer, flags);
        buffer.put(decimals);
        buffer.position(buffer.position() + FILLER.length);
        if (definition != null) {
            BufferUtils.writeWithLength(buffer, definition);
        }
    }

}
