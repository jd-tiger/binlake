package com.jd.binlog.conn;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.jd.binlog.mysql.PacketUtils;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.mysql.*;
import com.jd.binlog.util.CharsetUtils;

/**
 * 默认输出的数据编码为UTF-8，如有需要请正确转码
 *
 * @author jianghang 2013-9-4 上午11:50:26
 * @since 1.0.0
 */
public class MySQLExecutor {
    private static final Logger logger = Logger.getLogger(MySQLExecutor.class);
    // timeout : create mysql conn and execute sql under control
    public static final long EXECUTE_TIMEOUT = 2000L;
    // utf-8
    private static final int CHARSET_NUMBER = 33;

    private SocketChannel channel;

    public MySQLExecutor(MySQLConnector connector) {
        if (!connector.isConnected()) {
            throw new RuntimeException("should execute connector.connect() first");
        }

        this.channel = connector.getChannel();
    }

    public ResultSetPacket execute(final String sql) throws IOException {
        FutureTask<ResultSetPacket> future = new FutureTask<ResultSetPacket>(new Callable<ResultSetPacket>() {
            public ResultSetPacket call() throws Exception {
                return query(sql);
            }
        });
        MySQLExecuteService.connExecutor.execute(future);

        try {
            return future.get(EXECUTE_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            future.cancel(true);
            throw new IOException("sql: [" + sql + "] execute timeout");
        }
    }

    public void setConnCfg(String slaveUUID) throws BinlogException {
        MySQLUpdates upds = new MySQLUpdates(slaveUUID);
        update(upds.getSlaveUuid());
        update(upds.getNetReadTimeout());
        update(upds.getBinlogCheckSum());
        update(upds.getNames());
        update(upds.getNetWriteTimeout());
        update(upds.getWaitTimeout());
    }

    /**
     * (Result Set Header Packet) the number of columns <br>
     * (Field Packets) column descriptors <br>
     * (EOF Packet) marker: end of Field Packets <br>
     * (Row Data Packets) row contents <br>
     * (EOF Packet) marker: end of Data Packets
     *
     * @param sql
     * @return
     * @throws IOException
     */
    public ResultSetPacket query(String sql) throws IOException {
        logger.debug("query : " + sql);
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = MySQLPacket.COM_QUERY;
        cmd.arg = sql.getBytes(CharsetUtils.getCharset(CHARSET_NUMBER));
        cmd.write(channel.socket().getOutputStream());
        channel.socket().getOutputStream().flush();
        byte[] body = readNextPacket();

        if (body[0] < 0) {
            ErrorPacket packet = new ErrorPacket();
            packet.read(body);
            throw new IOException(packet + "\n with command: " + sql);
        }

        List<FieldPacket> fields = new ArrayList<FieldPacket>();
        body = readNextPacket();
        while (body[0] != EOFPacket.FIELD_COUNT) {
            FieldPacket fp = new FieldPacket();
            fp.read(body, true);
            fields.add(fp);
            body = readNextPacket();
        }

        body = readNextPacket();
        List<RowDataPacket> rows = new ArrayList<RowDataPacket>();
        if (body[0] == ErrorPacket.FIELD_COUNT) {
            ErrorPacket error = new ErrorPacket();
            error.read(body);
            if (error.errno == 1220) {
                throw new BinlogException(ErrorCode.WARN_QUERY_NO_FILE, new Exception(new String(error.message)), sql);
            }
            throw new IOException(new String(error.message));
        }
        while (body[0] != EOFPacket.FIELD_COUNT) {
            RowDataPacket row = new RowDataPacket(fields.size());
            row.read(body, true);
            rows.add(row);
            body = readNextPacket();
        }

        ResultSetPacket resultSet = new ResultSetPacket();
        resultSet.getFields().addAll(fields);
        List<String> fieldValues = resultSet.getFieldValues();
        for (RowDataPacket row : rows) {
            for (byte[] value : row.fieldValues) {
                if (value == null) {
                    fieldValues.add(null);
                } else if (value.length == 0) {
                    fieldValues.add(new String());
                } else {
                    fieldValues.add(new String(value));
                }
            }
        }
        resultSet.setSourceAddress(channel.socket().getRemoteSocketAddress());

        return resultSet;
    }

    public void update(String sql) throws BinlogException {
        logger.debug("query : " + sql);
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = MySQLPacket.COM_QUERY;
        StringBuilder msg = new StringBuilder();
        try {
            cmd.arg = sql.getBytes(CharsetUtils.getCharset(CHARSET_NUMBER));
            cmd.write(channel.socket().getOutputStream());
            channel.socket().getOutputStream().flush();
            byte[] body = readNextPacket();

            if (body[0] < 0) {
                ErrorPacket packet = new ErrorPacket();
                packet.read(body);
                msg.append(new String(packet.message)).append("\n with command: ").append(sql);
            }

            if (body[0] != OKPacket.FIELD_COUNT) {
                msg.append(sql).append(" with not OK return");
            }
            // update success without error
        } catch (Throwable exp) {
            throw new BinlogException(ErrorCode.WARN_MySQL_SET, exp, msg.toString());
        }
    }

    protected byte[] readNextPacket() throws IOException {
        HeaderPacket h = PacketUtils.readHeader(channel, MySQLConnector.PACKET_HEAD_SIZE);
        return PacketUtils.readBytes(channel, h.packetLength);
    }
}
