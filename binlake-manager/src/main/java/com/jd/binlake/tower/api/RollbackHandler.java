package com.jd.binlake.tower.api;

import com.jd.binlake.tower.zk.ZkService;
import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.conn.MySQLUpdates;
import com.jd.binlog.dbsync.*;
import com.jd.binlog.dbsync.event.GtidLogEvent;
import com.jd.binlog.dbsync.event.PreviousGtidsLogEvent;
import com.jd.binlog.dump.BinlogDump;
import com.jd.binlog.dump.DumpType;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.HeaderPacket;
import com.jd.binlog.mysql.OKPacket;
import com.jd.binlog.mysql.PacketUtils;
import com.jd.binlog.mysql.RegisterSlavePacket;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

/**
 * Created by jingdi on 17-8-23.
 * <p>
 * 根据时间戳回滚
 */
public class RollbackHandler extends AbstractHandler {
    private static final Logger logger = Logger.getLogger(RollbackHandler.class);
    private static final String ROLL_BACK = "/rollback";

    public static void register() {
        ApiCenter.register(ROLL_BACK, new RollbackHandler());
    }

    @Override
    public void handle(String s, Request request, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, ServletException {
        logger.debug("handle : " + s + ", request: " + request);
        Meta.MetaData metaData = ApiCenter.getPostMetaData(request, httpServletResponse);

        if (metaData == null) {
            return;
        }
        logger.info("request metadata : " + metaData);

        ZkService zkService = new ZkService(metaData.getZk().getServers(), metaData.getZk().getPath());
        MySQLConnector connector = null;
        try {
            String host = metaData.getDbInfo().getHost();
            int port = metaData.getDbInfo().getPort();
            //check host whether exist
            if (!zkService.hostExist(host, port + "")) {
                zkService.close();
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.INSTANCE_NOT_EXIST, "znode : " + host + ":" + port + " not exist"),
                        HttpServletResponse.SC_OK);
                return;
            }

            Meta.DbInfo dbInfo = zkService.getDbInfo(ApiCenter.makeZNodePath(host, port + ""));
            logger.debug("dbinfo : " + dbInfo);
            Meta.BinlogInfo binlogInfo = zkService.getBinlogInfo(ApiCenter.makeZNodePath(host, port + ""));
            logger.debug("binlog info : " + binlogInfo);
            MetaInfo metaInfo = new MetaInfo(dbInfo, binlogInfo);
            logger.debug("meta info : " + metaInfo);
            //Meta.BinlogInfo.Builder builder = binlogInfo.toBuilder();

            zkService.setOffline(host, port);

            if (binlogInfo.getBinlogWhen() <= metaData.getSlave().getBinlogWhen()) {
                ApiCenter.write(request, httpServletResponse,
                        ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                        HttpServletResponse.SC_OK);
                return;
            }

            connector = new MySQLConnector(metaInfo.getDbInfo());
            connector.handshake();

            String binlogFile = binlogInfo.getBinlogFile();
            logger.debug("binlog file : " + binlogFile);

            final LogDecoder decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);

            if (metaInfo.getBinlogInfo().getWithGTID()) {
                handleGtidDump(connector, decoder, metaInfo, binlogInfo, zkService, metaData);
            } else {
                handleCommonDump(connector, decoder, metaInfo, binlogInfo, zkService, metaData);
            }

            zkService.setOnline(host, port);

            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.SUCCESS, Constants.EMPTY),
                    HttpServletResponse.SC_OK);
        } catch (Exception e) {
            e.printStackTrace();
            ApiCenter.write(request, httpServletResponse,
                    ApiCenter.format(Constants.FAILURE, e.getMessage()),
                    HttpServletResponse.SC_OK);
        } finally {
            zkService.close();
            if (connector != null && connector.isConnected()) {
                connector.disconnect();
            }
        }
    }

    private void handleCommonDump(MySQLConnector connector, LogDecoder decoder, MetaInfo metaInfo, Meta.BinlogInfo binlogInfo, ZkService zkService, Meta.MetaData reqBuilder) throws Exception {
        boolean flag = false;
        String pointFileName = null;
        long pointLogPos = 4;
        String binlogFile = binlogInfo.getBinlogFile();
        final LogContext context = new LogContext();

        String host = reqBuilder.getDbInfo().getHost();
        int port = reqBuilder.getDbInfo().getPort();
        long time = reqBuilder.getSlave().getBinlogWhen();

        while (!flag) {
            if (!isBinlogExist(binlogFile, connector)) {
                throw new Exception("binlog file is not exist : " + binlogFile);
            }
            binlogInfo.setBinlogFile(binlogFile);
            updateConnectionProps(connector, metaInfo.getDbInfo().getSlaveUUID());
            registerSlave(connector, metaInfo);
            BinlogDump.sendDumpCommand(connector, DumpType.COM_BINLOG_DUMP, metaInfo);
            LogEvent currEvent = null;
            HeaderPacket header = null;
            LogEvent lastEvent = null;

            while (true) {
                header = PacketUtils.readHeader(connector.getChannel(), 4);
                byte[] data = PacketUtils.readBytes(connector.getChannel(), header.packetLength);
                LogBuffer buffer = new LogBuffer(data, 1, data.length - 1);
                currEvent = decoder.decode(buffer, context);

                if (currEvent != null && currEvent.getWhen() > time) {
                    if (lastEvent != null && lastEvent.getWhen() != 0) {
                        pointFileName = binlogFile;
                        pointLogPos = lastEvent.getLogPos();
                        flag = true;
                    } else {
                        binlogFile = getPreBinlogFile(binlogInfo.getBinlogFile());
                    }
                    break;
                }

                lastEvent = currEvent;
            }
            connector.disconnect();
            connector.handshake();
        }

        if (pointFileName != null) {
            zkService.setBinlogPosition(host, port, pointFileName, pointLogPos, "");
        }
    }

    private void handleGtidDump(MySQLConnector connector, LogDecoder decoder, MetaInfo metaInfo, Meta.BinlogInfo binlogInfo, ZkService zkService, Meta.MetaData reqBuilder) throws Exception {
        boolean flag = false;
        String pointFileName = null;
        long pointLogPos = 4;
        String binlogFile = binlogInfo.getBinlogFile();
        final LogContext context = new LogContext();

        String host = reqBuilder.getDbInfo().getHost();
        int port = reqBuilder.getDbInfo().getPort();
        long time = reqBuilder.getSlave().getBinlogWhen();

        Map<String, GTID> previousGtids = null;

        while (!flag) {
            if (!isBinlogExist(binlogFile, connector)) {
                throw new Exception("binlog file is not exist : " + binlogFile);
            }
            binlogInfo.setBinlogFile(binlogFile);
            updateConnectionProps(connector, metaInfo.getDbInfo().getSlaveUUID());
            registerSlave(connector, metaInfo);
            BinlogDump.sendDumpCommand(connector, DumpType.COM_BINLOG_DUMP, metaInfo);
            LogEvent currEvent = null;
            HeaderPacket header = null;
            LogEvent lastEvent = null;

            while (true) {
                header = PacketUtils.readHeader(connector.getChannel(), 4);
                byte[] data = PacketUtils.readBytes(connector.getChannel(), header.packetLength);
                LogBuffer buffer = new LogBuffer(data, 1, data.length - 1);
                currEvent = decoder.decode(buffer, context);

                if (currEvent != null && currEvent.getWhen() > time) {
                    if (lastEvent != null && lastEvent.getWhen() != 0) {
                        pointFileName = binlogFile;
                        pointLogPos = lastEvent.getLogPos();
                        flag = true;
                    } else {
                        binlogFile = getPreBinlogFile(binlogInfo.getBinlogFile());
                    }
                    break;
                }

                if (currEvent != null && currEvent.getHeader().getType() == LogEvent.PREVIOUS_GTIDS_LOG_EVENT) {
                    PreviousGtidsLogEvent previousGtidsLogEvent = (PreviousGtidsLogEvent) currEvent;
                    previousGtids = previousGtidsLogEvent.getPreGTIDs();
                }

                if (currEvent != null && isGtidEvent(currEvent) && previousGtids != null) {
                    GtidLogEvent gtidLogEvent = (GtidLogEvent) currEvent;
                    GTID newGtid = new GTID(gtidLogEvent.getGtidGNO(), gtidLogEvent.getSid());
                    addNewGtid(previousGtids, newGtid);
                }

                lastEvent = currEvent;
            }
            connector.disconnect();
            connector.handshake();
        }

        if (previousGtids != null) {
            zkService.setBinlogPosition(host, port, pointFileName, pointLogPos, handleGtidMap(previousGtids));
        }
    }


    private boolean isBinlogExist(String binlogFile, MySQLConnector connector)
            throws IOException {
        MySQLExecutor executor = new MySQLExecutor(connector);

        try {
            executor.query("SHOW BINLOG EVENTS IN '" + binlogFile + "' FROM 4" + " LIMIT 1");
        } catch (BinlogException exp) {
            // ERROR 1220 (HY000): Error when executing command SHOW BINLOG EVENTS: Could not find target log
            logger.debug(ExceptionUtils.getStackTrace(exp));
            return false;
        }
        return true;
    }

    private void registerSlave(MySQLConnector connector, MetaInfo metaInfo) throws IOException {
        logger.error("register slave");
        RegisterSlavePacket rsp = new RegisterSlavePacket();
        rsp.slaveId = metaInfo.getSlaveId();
        rsp.write(connector.getChannel().socket().getOutputStream());
        HeaderPacket header = null;
        header = PacketUtils.readHeader(connector.getChannel(), 4);
        byte[] bytes = PacketUtils.readBytes(connector.getChannel(), header.packetLength);
        switch (bytes[0]) {
            case OKPacket.FIELD_COUNT:
                return;
            default:
                throw new IOException("register slave error");
        }
    }

    private void updateConnectionProps(MySQLConnector connector, String slaveUUID) throws IOException {
        MySQLExecutor mysqlExecutor = new MySQLExecutor(connector);
        MySQLUpdates upds = new MySQLUpdates(slaveUUID);

        mysqlExecutor.update(upds.getSlaveUuid());
        mysqlExecutor.update(upds.getBinlogCheckSum());
        mysqlExecutor.update(upds.getNetWriteTimeout());
        mysqlExecutor.update(upds.getNetReadTimeout());
        mysqlExecutor.update(upds.getWaitTimeout());
        mysqlExecutor.update(upds.getNames());
    }

    private String getPreBinlogFile(String binlogFile) {
        int length = binlogFile.length();
        String prefix = binlogFile.substring(0, length - 6);
        String suffix = binlogFile.substring(length - 6, length);
        long idx = Long.parseLong(suffix);
        return prefix + String.format("%06d", idx - 1);
    }

    private boolean isGtidEvent(LogEvent event) {
        int eventType = event.getHeader().getType();
        switch (eventType) {
            case LogEvent.ANONYMOUS_GTID_LOG_EVENT:
            case LogEvent.GTID_LOG_EVENT:
                return true;
        }
        return false;
    }

    private void addNewGtid(Map<String, GTID> gtids, GTID newGtid) {
        String sid = newGtid.sid;
        if (gtids.containsKey(sid)) {
            GTID gtid = gtids.get(sid);
            newGtid.mergeGtid(gtid);
            gtids.put(sid, newGtid);
            return;
        }

        gtids.put(sid, newGtid);
    }

    private String handleGtidMap(Map<String, GTID> gtids) {
        StringBuilder gtidSet = new StringBuilder();
        for (Map.Entry<String, GTID> entry : gtids.entrySet()) {
            gtidSet.append(entry.getValue().toString()).append(",");
        }
        gtidSet.deleteCharAt(gtidSet.length() - 1);

        return gtidSet.toString();
    }

}
