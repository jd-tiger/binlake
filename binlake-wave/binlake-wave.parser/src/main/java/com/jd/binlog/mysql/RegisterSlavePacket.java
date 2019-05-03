package com.jd.binlog.mysql;

import com.jd.binlog.util.StreamUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created by pengan on 17-3-16.
 * <p>
 * <p>
 * <p>
 * 1              [15] COM_REGISTER_SLAVE
 * 4              server-id
 * 1              slaves hostname length
 * string[$len]   slaves hostname
 * 1              slaves user len
 * string[$len]   slaves user
 * 1              slaves password len
 * string[$len]   slaves password
 * 2              slaves mysql-port
 * 4              replication rank
 * 4              master-id
 */

public class RegisterSlavePacket extends MySQLPacket {
    public static final int FIELD_COUNT = 18;
    private final byte command = COM_REGISTER_SLAVE;

    /**
     * fixed host and port
     */
    public long slaveId;

    /**
     * write in output stream
     *
     * @param out
     * @throws IOException
     */
    public void write(OutputStream out) throws IOException {
        StreamUtils.writeUB3(out, calcPacketSize());
        StreamUtils.write(out, packetId);
        StreamUtils.write(out, command);
        StreamUtils.writeUB4(out, slaveId);
        StreamUtils.write(out, (byte) 0);
        StreamUtils.write(out, (byte) 0);
        StreamUtils.write(out, (byte) 0);

        StreamUtils.writeUB2(out, 0);
        StreamUtils.writeUB4(out, 0);
        StreamUtils.writeUB4(out, 0);
    }

    /**
     * get command args : convert register slave to command
     *
     * @return
     */
    public CommandPacket getCommandPacket() {
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = command;
        byte[] args = new byte[calcPacketSize() - 1];

        int index = 0;
        args[index++] = (byte) (slaveId & 0xff);
        args[index++] = (byte) (slaveId >>> 8);
        args[index++] = (byte) (slaveId >>> 16);
        args[index++] = (byte) (slaveId >>> 24);

        args[index++] = (byte) 0;
        args[index++] = (byte) 0;
        args[index++] = (byte) 0;

        args[index++] = (byte) 0;
        args[index++] = (byte) 0;

        args[index++] = (byte) 0;
        args[index++] = (byte) 0;
        args[index++] = (byte) 0;
        args[index++] = (byte) 0;

        args[index++] = (byte) 0;
        args[index++] = (byte) 0;
        args[index++] = (byte) 0;
        args[index++] = (byte) 0;

        cmd.arg = args;
        return cmd;
    }

    public int calcPacketSize() {
        return 1 + 4 + 1 + 1 + 1 + 2 + 4 + 4;
    }

    protected String getPacketInfo() {
        return "register slave";
    }
}
