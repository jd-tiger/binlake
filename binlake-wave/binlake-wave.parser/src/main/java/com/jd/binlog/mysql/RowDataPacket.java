package com.jd.binlog.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by pengan on 16-11-4.
 */
public class RowDataPacket extends MySQLPacket {
    public static final byte NULL_MARK = (byte) 0xfb;
    public static final byte EMPTY_MARK = (byte) 0x00;

    public int fieldCount;
    public List<byte[]> fieldValues;

    public RowDataPacket(int fieldCount) {
        this.fieldCount = fieldCount;
        this.fieldValues = new ArrayList<byte[]>(fieldCount);
    }

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        getFieldValue(mm);
    }

    public void read(byte[] data, boolean noHeader) {
        MySQLMessage mm = new MySQLMessage(data);
        getFieldValue(mm);
    }

    private void getFieldValue(MySQLMessage mm) {
        for (int i = 0; i < fieldCount; i++) {
            fieldValues.add(mm.readBytesWithLength());
        }
    }

    @Override
    public int calcPacketSize() {//TODO 这里可能需要重新计算一下，因为可能会涉及到有些字段需要剔除
        int size = 0;
        for (int i = 0; i < fieldCount; i++) {
            byte[] v = fieldValues.get(i);
            size += (v == null || v.length == 0) ? 1 : v.length;
        }
        return size;
    }


    public int calcPacketSize(Set<Integer> delete) {
        int size = 0;
        //TODO
        for (int i = 0; i < fieldCount; i++) {
            if (delete.contains(i))
                continue;

            byte[] v = fieldValues.get(i);
            size += (v == null || v.length == 0) ? 1 : v.length;
        }

        return size;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL RowData Packet";
    }

}
