package com.jd.binlog.domain;

import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.dbsync.LogBuffer;
import com.jd.binlog.dbsync.LogContext;
import com.jd.binlog.dbsync.LogDecoder;
import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.event.QueryLogEvent;
import com.jd.binlog.dbsync.event.RotateLogEvent;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.HeaderPacket;
import com.jd.binlog.mysql.PacketUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static com.jd.binlog.inter.msg.IConvert.BEGIN;
import static com.jd.binlog.inter.msg.IConvert.COMMIT;


/**
 * Created by pengan on 17-2-23.
 * <p/>
 * back trackEvent using timestamp
 * <p/>
 * get the nearest binlog begin
 */
public class TimeTracker extends DomainSwitchTracker {

    /**
     * 如果lastEventTime 永远大于当前最大binlog 文件当中的任何事件，那么不可能总是等待binlog事件到来
     * <p/>
     * 此时需要有超时设置，超时时间设置成2s
     * <p/>
     * 如果超过2s，那么就认定为当前dump的最后的logEvent为最符合的event
     * <p/>
     * 如果有begin事件，那么就认为是最接近的logEvent
     */
    private static final int EXECUTE_TIMEOUT = 1800000;
    private LogEvent nearestEvent;
    private LogEvent nearestCommit;

    public TimeTracker(MetaInfo metaInfo, HttpConfig httpConf) {
        super(metaInfo, httpConf);
    }

    public void trackEvent() throws IOException {
        trackEvent(false);
    }

    private void trackEvent(final boolean isTransaction) throws IOException {
        final MySQLConnector connector = new MySQLConnector(metaInfo.getDbInfo());
        connector.handshake();

        getNewBinlogOffset(metaInfo, connector);

        final LogDecoder decoder = new LogDecoder();
        decoder.handle(LogEvent.ROTATE_EVENT);
        decoder.handle(LogEvent.FORMAT_DESCRIPTION_EVENT);
        decoder.handle(LogEvent.QUERY_EVENT);
        decoder.handle(LogEvent.XID_EVENT);

        final LogContext context = new LogContext();
        String binlogFile = binlogInfo.getBinlogFile();
        boolean firstFlag = true;

        do {
            if (!isBinlogExist(binlogFile, connector)) {
                // binlog file not exist use the previous binlogInfo
                break;
            }
            binlogInfo.setBinlogFile(binlogFile);
            updateConnectionProps(connector);
            registerSlave(connector);
            sendDumpCommand(metaInfo.getSlaveId(), binlogInfo, connector);

            if (!firstFlag) { // mostly call this
                if (isTransaction) {
                    if (isNearestCommit(decoder, binlogInfo, connector, context)) {
                        break;
                    }
                } else {
                    if (isNearestEvent(decoder, binlogInfo, connector, context)) {
                        break;
                    }
                }
            } else {
                /**
                 * 只有首次查找binlog的时候才需要开启future task
                 *
                 * 原因： 有可能从库最新的binlog时间都比主库dump的时间戳小，避免长时间等待
                 *
                 */
                firstFlag = false;
                FutureTask<Boolean> future = new FutureTask<Boolean>(new Callable<Boolean>() {
                    public Boolean call() throws IOException {
                        if (isTransaction) {
                            if (isNearestCommit(decoder, binlogInfo, connector, context)) {
                                return true;
                            }
                        } else {
                            if (isNearestEvent(decoder, binlogInfo, connector, context)) {
                                return true;
                            }
                        }
                        return false;
                    }
                });
                switchExecutors.execute(future);

                try {
                    if (future.get(EXECUTE_TIMEOUT, TimeUnit.MILLISECONDS)) {
                        break;
                    }
                } catch (Throwable e) {
                    future.cancel(true);

                    // timeout exception
                    if (nearestCommit != null) {
                        // considering using transaction primarily
                        LogEvent event = nearestCommit;
                        binlogInfo.setBinlogPos(event.getLogPos());
                        binlogInfo.setBinlogWhen(event.getWhen());
                        break;
                    }
                    /**
                     * 如果一开始就超时异常 并且记录的事件都为null
                     */
                    binlogInfo.setBinlogPos(DEFAULT_BINLOG_START_OFFSET);
                    break;
                }
            }

            binlogFile = getPreBinlogFile(binlogInfo.getBinlogFile());
            connector.disconnect();
            connector.handshake();
        } while (true);

        connector.disconnect();
    }

    /**
     * find the nearest begin where begin.when() <= target.when()
     *
     * @param decoder
     * @param binlogInfo
     * @param connector
     * @param context
     * @return
     * @throws IOException connect failed or time exceed
     */
    private boolean isNearestCommit(LogDecoder decoder,
                                    Meta.BinlogInfo binlogInfo,
                                    MySQLConnector connector,
                                    LogContext context) throws IOException {
        boolean isNearestCommit = false;
        LogEvent preCommit = null;
        LogEvent currEvent = null;
        HeaderPacket header = null;

        // each time just on one binlog file, what to find is the previous begin
        while (true) {
            header = PacketUtils.readHeader(connector.getChannel(), 4);
            byte[] data = PacketUtils.readBytes(connector.getChannel(), header.packetLength);
            LogBuffer buffer = new LogBuffer(data, 1, data.length - 1);
            currEvent = decoder.decode(buffer, context);

            if (currEvent == null) {
                // current begin == null error
                throw new IOException("current begin == null , " + binlogInfo);
            }
            nearestEvent = currEvent;

            if (currEvent instanceof RotateLogEvent) {
                RotateLogEvent rotate = (RotateLogEvent) currEvent;

                if (rotate.getHeader().getWhen() == 0) {
                    // fake rotate begin just continue reference : https://dev.mysql.com/doc/internals/en/binlog-network-stream.html
                    continue;
                }

                // 说明当前binlog file 当中所有事件 ts < target.ts
                if (preCommit != null) {
                    binlogInfo.setBinlogPos(preCommit.getLogPos());
                    binlogInfo.setBinlogWhen(preCommit.getWhen());
                    isNearestCommit = true;
                    break;
                }
                // come with rotate event then break
                isNearestCommit = false;
                break;
            }

            if (currEvent.getWhen() >= lastEventTime) {
                /**
                 * binlog event and event.when() >= target.when()
                 *
                 * search for the previous binlog file
                 */
                if (currEvent instanceof QueryLogEvent) {
                    String queryString = ((QueryLogEvent) currEvent).getQuery();

                    if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
                        // here to record the first begin binlog event
                        if (preCommit == null) {
                            // 说明当前binlog file当中最开始的BEGIN > target.when()
                            isNearestCommit = false;
                            break;
                        }

                        // 说明目标binlog事件 属于上一个事务
                        binlogInfo.setBinlogPos(preCommit.getLogPos());
                        binlogInfo.setBinlogWhen(preCommit.getWhen());
                        isNearestCommit = true;
                        break;
                    }
                }

                if (preCommit != null) { // 说明目标binlog事件 属于当前事务 并且当前的binlog file当中存在 ts > target.ts
                    binlogInfo.setBinlogPos(preCommit.getLogPos());
                    binlogInfo.setBinlogWhen(preCommit.getWhen());
                    isNearestCommit = true;
                    break;
                }
                binlogInfo.setBinlogPos(DEFAULT_BINLOG_START_OFFSET);
                isNearestCommit = false;
                break;
            }
            int eventType = currEvent.getHeader().getType();

            switch (eventType) {
                case LogEvent.QUERY_EVENT:
                    LogUtils.debug.debug("EVENT : write");
                    String queryString = ((QueryLogEvent) currEvent).getQuery();

                    if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
                        // here to record the first begin binlog event
                        preCommit = currEvent;
                        nearestCommit = currEvent;
                    }
                    break;
                case LogEvent.XID_EVENT:
                    preCommit = currEvent;
                    nearestCommit = currEvent;
                    break;
                default:
                    break;
            }
        }
        return isNearestCommit;
    }

    public void trackTransaction() throws IOException {
        trackEvent(true);
    }

    /**
     * get nearest binlog begin :
     * <p>begin.when < previous.begin.when</p>
     * <p>min gap for (previous.begin.when - begin.when)</p>
     *
     * @param decoder
     * @param binlogInfo
     * @param connector
     * @param context
     * @return
     * @throws IOException
     */
    private boolean isNearestEvent(LogDecoder decoder,
                                   Meta.BinlogInfo binlogInfo,
                                   MySQLConnector connector,
                                   LogContext context) throws IOException {
        boolean isNearestEvent = false;
        LogEvent preEvent = null;
        LogEvent currEvent = null;
        HeaderPacket header = null;

        while (true) {
            header = PacketUtils.readHeader(connector.getChannel(), 4);
            byte[] data = PacketUtils.readBytes(connector.getChannel(), header.packetLength);
            LogBuffer buffer = new LogBuffer(data, 1, data.length - 1);
            currEvent = decoder.decode(buffer, context);

            if (currEvent == null) {
                // current begin == null error
                throw new IOException("current begin == null , " + binlogInfo);
            }
            nearestEvent = currEvent;

            if (currEvent instanceof RotateLogEvent) {
                RotateLogEvent rotate = (RotateLogEvent) currEvent;

                if (rotate.getHeader().getWhen() == 0) {
                    // fake rotate begin just continue reference : https://dev.mysql.com/doc/internals/en/binlog-network-stream.html
                    continue;
                }

                // not fake rotate begin and is the rotate begin preEvent could not be null
                binlogInfo.setBinlogPos(preEvent.getLogPos());
                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("rotate event when : " + currEvent.getWhen() +
                            ", target when : " + binlogInfo.getBinlogWhen());
                }

                isNearestEvent = true;
                break;
            }

            if (currEvent.getWhen() >= binlogInfo.getBinlogWhen()) {
                // close this channel and look for the previous binlog file
                if (preEvent == null) { // the 1st binlog begin
                    binlogInfo.setBinlogPos(DEFAULT_BINLOG_START_OFFSET); // it is the 1st begin
                    isNearestEvent = false;
                    break;
                }
                binlogInfo.setBinlogPos(preEvent.getLogPos());

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("binlog event when : " + currEvent.getWhen() +
                            ", target when : " + binlogInfo.getBinlogWhen());
                }

                isNearestEvent = true;
                break;
            } else {
                isNearestEvent = false;
                preEvent = currEvent;
            }
        }
        return isNearestEvent;
    }

    private void out(String msg) {
        System.err.println(msg);
    }
}
