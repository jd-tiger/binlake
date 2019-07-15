package com.jd.binlog.conn;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import com.jd.binlog.mysql.PacketUtils;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.mysql.*;
import com.jd.binlog.util.CharsetUtils;
import com.jd.binlog.util.SecurityUtils;

/**
 * 基于mysql socket协议的链接实现
 *
 * @author jianghang 2013-2-18 下午09:22:30
 * @version 1.0.1
 */
public class MySQLConnector {
    private static final Logger logger = Logger.getLogger(MySQLConnector.class);
    public static final int PACKET_HEAD_SIZE = 4;
    public static final int MAX_PACKET_SIZE = 16 * 1024 * 1024; // 16M
    public static final long CLIENT_FLAGS = initClientFlags();

    private static byte charsetNumber = 33;
    private String defaultSchema;
    private int soTimeout = 30 * 1000;
    private int receiveBufferSize = 128 * 1024;
    private int sendBufferSize = 64 * 1024;

    private String username;
    private String password;
    private long connectionId;
    private SocketChannel channel;
    private InetSocketAddress address;
    private AtomicBoolean connected = new AtomicBoolean(false);

    public MySQLConnector(Meta.DbInfo dbInfo) {
        this(new InetSocketAddress(dbInfo.getHost(), dbInfo.getPort()),
                dbInfo.getUser(), dbInfo.getPassword(), null);
    }

    public MySQLConnector(InetSocketAddress address, String username, String password, String databaseName) {
        this(address, username, password, charsetNumber, databaseName);
    }

    public MySQLConnector(InetSocketAddress address, String username, String password, byte charsetNumber,
                          String defaultSchema) {
        this.address = address;
        this.username = username;
        this.password = password;
        this.defaultSchema = defaultSchema;
        this.charsetNumber = charsetNumber;
    }

    public void connect() throws BinlogException {
        FutureTask<Void> future = new FutureTask<Void>(() -> {
            handshake();
            return null;
        });

        MySQLExecuteService.connExecutor.execute(future);
        try {
            future.get(MySQLExecutor.EXECUTE_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (Throwable e) {
            future.cancel(true);
            throw new BinlogException(ErrorCode.WARN_MySQL_HANDSHAKE, e, username + "/****");
        }
    }

    public void handshake() throws IOException {
        if (connected.compareAndSet(false, true)) {
            try {
                channel = SocketChannel.open();
                setSocketOpts(channel);
                logger.info("connect MysqlConnection to " + address);
                channel.connect(address);
                negotiate(channel);
            } catch (Exception e) {
                disconnect();
                throw new BinlogException(ErrorCode.WARN_MySQL_HANDSHAKE, e, "connect " + address + " failure:");
            }
        } else {
            logger.error("the channel can't be connected twice.");
        }
    }

    public void reconnect() throws IOException {
        disconnect();
        connect();
    }

    public void disconnect() throws IOException {
        if (connected.compareAndSet(true, false)) {
            try {
                if (channel != null) {
                    channel.close();
                }

                logger.info("disConnect MysqlConnection to " + address);
            } catch (Exception e) {
                throw new IOException("disconnect " + this.address + " failure:" + ExceptionUtils.getStackTrace(e));
            }
        } else {
            logger.info("the channel " + address + " is not connected");
        }
    }

    public boolean isConnected() {
        return this.channel != null && this.channel.isConnected();
    }

    private void setSocketOpts(SocketChannel channel) throws IOException {
        channel.socket().setKeepAlive(true);
        channel.socket().setReuseAddress(true);
        channel.socket().setSoTimeout(soTimeout);
        channel.socket().setTcpNoDelay(true);
        channel.socket().setReceiveBufferSize(receiveBufferSize);
        channel.socket().setSendBufferSize(sendBufferSize);
    }

    private void negotiate(SocketChannel channel) throws IOException, NoSuchAlgorithmException {
        HeaderPacket header = PacketUtils.readHeader(channel, 4);
        byte[] body = PacketUtils.readBytes(channel, header.packetLength);
        if (body[0] < 0) {// check field_count
            if (body[0] == -1) {
                ErrorPacket error = new ErrorPacket();
                throw new IOException("handshake exception:\n" + error.toString());
            } else if (body[0] == -2) {
                throw new IOException("Unexpected EOF packet at handshake phase.");
            } else {
                throw new IOException("unpexpected packet with field_count=" + body[0]);
            }
        }
        HandshakePacket hsp = new HandshakePacket();
        hsp.read(body);
        connectionId = hsp.threadId;

        AuthPacket ap = new AuthPacket();
        ap.packetId = 1;
        ap.clientFlags = CLIENT_FLAGS;
        ap.maxPacketSize = MAX_PACKET_SIZE;
        ap.charsetIndex = charsetNumber;
        ap.user = username;
        encryptPassword(hsp, ap, password, charsetNumber);

        ap.write(channel.socket().getOutputStream());

        header = PacketUtils.readHeader(channel, 4);
        body = PacketUtils.readBytes(channel, header.packetLength);
        if (body == null) {
            throw new IOException("unexpected null packet from MySQL");
        }
        if (body[0] < 0) {
            if (body[0] == -1) {
                ErrorPacket err = new ErrorPacket();
                throw new IOException("Error When doing Client Authentication:" + err.toString());
            } else {
                throw new IOException("unexpected packet with field_count=" + body[0]);
            }
        }
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public SocketChannel getChannel() {
        return channel;
    }

    public long getConnectionId() {
        return connectionId;
    }

    private static long initClientFlags() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        // flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= Capabilities.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        // client extension
        // flag |= Capabilities.CLIENT_MULTI_STATEMENTS;
        // flag |= Capabilities.CLIENT_MULTI_RESULTS;
        return flag;
    }

    public static void encryptPassword(HandshakePacket hsp, AuthPacket ap, String passwd, int charsetIndex) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        if (passwd != null && passwd.length() > 0) {
            byte[] password = passwd.getBytes(CharsetUtils.getCharset(charsetIndex));
            byte[] seed = hsp.seed;
            byte[] restOfScramble = hsp.restOfScrambleBuff;
            byte[] authSeed = new byte[seed.length + restOfScramble.length];
            System.arraycopy(seed, 0, authSeed, 0, seed.length);
            System.arraycopy(restOfScramble, 0, authSeed, seed.length,
                    restOfScramble.length);
            ap.password = SecurityUtils.scramble411(password, authSeed);
        }
    }
}
