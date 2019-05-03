package com.jd.binlog.mysql;


import com.jd.binlog.util.StreamUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Created by pengan on 16-10-6.
 */
public class CommandPacket extends MySQLPacket {

    public byte command;
    public byte[] arg;

    public void read(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data);
        packetLength = mm.readUB3();
        packetId = mm.read();
        command = mm.read();
        arg = mm.readBytes();
    }

    public void write(OutputStream out) throws IOException {
        StreamUtils.writeUB3(out, calcPacketSize());
        StreamUtils.write(out, packetId);
        StreamUtils.write(out, command);
        StreamUtils.writeBytes(out, arg);
    }

    @Override
    public int calcPacketSize() {
        return 1 + arg.length;
    }

    @Override
    protected String getPacketInfo() {
        return "MySQL Command Packet";
    }

    @Override
    public String toString() {
        try {
            return new String(arg, "utf8");
        } catch (UnsupportedEncodingException e) {
            return new String(arg);
        }
    }
}
