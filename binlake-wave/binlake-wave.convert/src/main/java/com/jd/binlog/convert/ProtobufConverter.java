package com.jd.binlog.convert;

import com.google.protobuf.ByteString;
import com.jd.binlog.dbsync.event.*;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.inter.filter.IFilter;
import com.jd.binlog.inter.msg.IConvert;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.msg.IRepartition;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IKeyGenerator;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.parser.SimpleDdlParser;
import com.jd.binlog.parser.TableMeta;
import com.jd.binlog.protocol.WaveEntry;
import com.jd.binlog.util.CharsetUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created on 18-7-16
 * <p>
 * protobuf 格式转换器 数据的转换中心
 *
 * @author pengan
 */
public class ProtobufConverter implements IConvert<String> {
    @Override
    public void appendFakeColumns(IFilter filter, BitSet bs, IMessage msg) {
        msg.addFakeColumn(filter.getFakeCols(), bs);
    }

    @Override
    public void formatUpdateByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        WaveEntry.EventType eventType = WaveEntry.EventType.UPDATE;
        formatByOneRow(msg, id, generator, producer, part, eventType, worker);
    }

    @Override
    public void formatInsertByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        WaveEntry.EventType eventType = WaveEntry.EventType.INSERT;
        formatByOneRow(msg, id, generator, producer, part, eventType, worker);
    }

    @Override
    public void formatDeleteByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        WaveEntry.EventType eventType = WaveEntry.EventType.DELETE;
        formatByOneRow(msg, id, generator, producer, part, eventType, worker);
    }

    @Override
    public void formatBeginQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        String binlogFile = msg.getBinlogFile();
        QueryLogEvent event = (QueryLogEvent) msg.getLogEvent();

        WaveEntry.TransactionBegin begin = createTransactionBegin(event.getSessionId());
        WaveEntry.Header header = createHeader(binlogFile, "", "", event.getHeader(), null);

        WaveEntry.Entry entry = createEntry(header,
                WaveEntry.EntryType.TRANSACTIONBEGIN,
                begin.toByteString(),
                msg.getHost());

        LogUtils.debug.debug(entry);

        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatCommitQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        //XidLogEvent event = (XidLogEvent) msg.getLogEvent();
        //WaveEntry.TransactionEnd transactionEnd = createTransactionEnd(event.getXid());
        QueryLogEvent event = (QueryLogEvent) msg.getLogEvent();
        WaveEntry.TransactionEnd transactionEnd = createTransactionEnd(0L);
        WaveEntry.Header header = createHeader(msg.getBinlogFile(), "", "", event.getHeader(), null);

        WaveEntry.Entry entry = createEntry(header,
                WaveEntry.EntryType.TRANSACTIONEND,
                transactionEnd.toByteString(),
                msg.getHost());

        LogUtils.debug.debug(entry);

        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatDDLQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        QueryLogEvent event = (QueryLogEvent) msg.getLogEvent();

        String query = event.getQuery();

        // DDL语句处理
        SimpleDdlParser.DdlResult result = SimpleDdlParser.parse(query, event.getDbName());

        String schemaName = event.getDbName();
        if (StringUtils.isNotEmpty(result.getSchemaName())) {
            schemaName = result.getSchemaName();
        }

        String tableName = result.getTableName();
        //additional filter
        WaveEntry.EventType type = WaveEntry.EventType.QUERY;
        // fixed issue https://github.com/alibaba/canal/issues/58

        switch (result.getType()) {
            case ALTER:
            case ERASE:
            case CREATE:
            case TRUNCATE:
            case RENAME:
            case CINDEX:
            case DINDEX:
                type = result.getType();
                if (StringUtils.isEmpty(tableName) ||
                        (result.getType() == WaveEntry.EventType.RENAME && StringUtils.isEmpty(result.getOriTableName()))) {
                    // 如果解析不出tableName,记录一下日志，方便bugfix，目前直接抛出异常，中断解析
                    throw new BinlogException(ErrorCode.WARN_MySQL_DDL_PARSE,
                            new Exception("SimpleDdlParser process write failed. pls submit issue with this queryString: "
                                    + query + " , and DdlResult: " + result.toString()), query);
                }
                break;
            case INSERT:
            case DELETE:
            case UPDATE:
                // 对外返回，保证兼容，还是返回QUERY类型，这里暂不解析tableName，所以无法支持过滤
                // 采集的日志格式为 statement 格式
                LogUtils.warn.warn("host " + msg.getHost() + " binlog format = statement");
                break;

        }

        WaveEntry.Header header = createHeader(msg.getBinlogFile(), schemaName, tableName, event.getHeader(), type);
        WaveEntry.RowChange.Builder rowChangeBuider = WaveEntry.RowChange.newBuilder();
        if (result.getType() != WaveEntry.EventType.QUERY) {
            rowChangeBuider.setIsDdl(true);
        }
        rowChangeBuider.setSql(query);
        if (StringUtils.isNotEmpty(event.getDbName())) {// 可能为空
            rowChangeBuider.setDdlSchemaName(event.getDbName());
        }
        rowChangeBuider.setEventType(result.getType());
        WaveEntry.Entry entry = createEntry(header,
                WaveEntry.EntryType.ROWDATA,
                rowChangeBuider.build().toByteString(),
                msg.getHost());

        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatCommitXID(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        XidLogEvent event = (XidLogEvent) msg.getLogEvent();
        WaveEntry.TransactionEnd commit = createTransactionEnd(event.getXid());

        WaveEntry.Header header = createHeader(msg.getBinlogFile(), "", "", event.getHeader(), null);

        WaveEntry.Entry entry = createEntry(header,
                WaveEntry.EntryType.TRANSACTIONEND,
                commit.toByteString(),
                msg.getHost());

        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatRowsQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        RowsQueryLogEvent event = (RowsQueryLogEvent) msg.getLogEvent();

        // mysql5.6支持，需要设置binlog-rows-write-log-events=1，可详细打印原始DML语句
        String query = null;
        try {
            query = new String(event.getRowsQuery().getBytes(ISO_8859_1), CharsetUtils.DEFAULT_CHARSET);
            WaveEntry.Entry entry = buildQueryEntry(query, msg.getHost(), msg.getBinlogFile(), event.getHeader());
            part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
        } catch (UnsupportedEncodingException e) {
            throw new BinlogException(ErrorCode.ERR_UNSUPPORT_ENCODE, e, "charset:" + ISO_8859_1 + "|offset{" + msg.getBinlogFile() + ":" + msg.getLogPosition() + "}");
        }
    }

    @Override
    public void formatUserVar(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        UserVarLogEvent event = (UserVarLogEvent) msg.getLogEvent();

        WaveEntry.Entry entry = buildQueryEntry(event.getQuery(), msg.getHost(), msg.getBinlogFile(), event.getHeader());
        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatIntVar(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        IntvarLogEvent event = (IntvarLogEvent) msg.getLogEvent();
        WaveEntry.Entry entry = buildQueryEntry(event.getQuery(), msg.getHost(), msg.getBinlogFile(), event.getHeader());
        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatRand(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        RandLogEvent event = (RandLogEvent) msg.getLogEvent();
        WaveEntry.Entry entry = buildQueryEntry(event.getQuery(), msg.getHost(), msg.getBinlogFile(), event.getHeader());

        part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
    }

    @Override
    public void formatInsertByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        LogUtils.debug.debug("formatInsertByRows");
        WaveEntry.EventType eventType = WaveEntry.EventType.INSERT;
        formatByRows(msg, topic, generator, producer, part, eventType, worker);
    }

    @Override
    public void formatUpdateByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        LogUtils.debug.debug("formatUpdateByRows");
        WaveEntry.EventType eventType = WaveEntry.EventType.UPDATE;
        formatByRows(msg, topic, generator, producer, part, eventType, worker);
    }

    @Override
    public void formatDeleteByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {
        LogUtils.debug.debug("formatDeleteByRows");
        WaveEntry.EventType eventType = WaveEntry.EventType.DELETE;
        formatByRows(msg, topic, generator, producer, part, eventType, worker);
    }

    /**
     * 封装成一个 retValue 消息体 不增加 binlog 位置的引用计数器
     *
     * @param entry
     * @param generator
     * @param msg
     * @param id
     * @return
     */
    private IRule.RetValue<String> encapWithNoIncr(WaveEntry.Entry entry,
                                                   IKeyGenerator generator,
                                                   IProducer producer,
                                                   IMessage msg, String id,
                                                   IBinlogWorker worker) {
        IRule.RetValue<String> rv = new IRule.RetValue<String>();
        rv.id = id;
        rv.value = entry.toByteArray();
        rv.producer = producer;
        rv.worker = worker;
        rv.logPos = msg.getLogPosition();

        // 生成 key 值
        generator.generate(rv, msg);
        return rv;
    }

    /**
     * 事件已经拆包 所以需要增加计数器
     *
     * @param entry
     * @param generator
     * @param producer
     * @param msg
     * @param id
     * @param worker
     * @return
     */
    private IRule.RetValue<String> encapWithIncr(WaveEntry.Entry entry,
                                                 IKeyGenerator generator,
                                                 IProducer producer,
                                                 IMessage msg, String id,
                                                 IBinlogWorker worker) {
        IRule.RetValue<String> rv = encapWithNoIncr(entry, generator, producer, msg, id, worker);
        rv.logPos.increment();
        return rv;
    }


    /**
     * @param queryString
     * @param host
     * @param binlogFile
     * @param logHeader
     * @return
     */
    private WaveEntry.Entry buildQueryEntry(String queryString, String host,
                                            String binlogFile, LogHeader logHeader) {
        WaveEntry.Header header = createHeader(binlogFile, "", "", logHeader, WaveEntry.EventType.QUERY);
        WaveEntry.RowChange.Builder rowChangeBuider = WaveEntry.RowChange.newBuilder();
        rowChangeBuider.setSql(queryString);
        rowChangeBuider.setEventType(WaveEntry.EventType.QUERY);
        return createEntry(header, WaveEntry.EntryType.ROWDATA, rowChangeBuider.build().toByteString(), host);
    }


    /**
     * @param threadId
     * @return
     */
    private WaveEntry.TransactionBegin createTransactionBegin(long threadId) {
        WaveEntry.TransactionBegin.Builder beginBuilder = WaveEntry.TransactionBegin.newBuilder();
        beginBuilder.setThreadId(threadId);
        return beginBuilder.build();
    }

    /**
     * @param transactionId
     * @return
     */
    private WaveEntry.TransactionEnd createTransactionEnd(long transactionId) {
        WaveEntry.TransactionEnd.Builder endBuilder = WaveEntry.TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(transactionId));
        return endBuilder.build();
    }

    /**
     * @param header
     * @param entryType
     * @param storeValue
     * @param host
     * @return
     */
    private WaveEntry.Entry createEntry(WaveEntry.Header header,
                                        WaveEntry.EntryType entryType,
                                        ByteString storeValue,
                                        String host) {
        WaveEntry.Entry.Builder entryBuilder = WaveEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        entryBuilder.setIp(host);
        return entryBuilder.build();
    }

    /**
     * 创建protobuf header
     *
     * @param binlogFile
     * @param db
     * @param table
     * @param logHeader
     * @param eventType
     * @return
     */
    private WaveEntry.Header createHeader(String binlogFile,
                                          String db,
                                          String table,
                                          LogHeader logHeader,
                                          WaveEntry.EventType eventType) {
        // header会做信息冗余,方便以后做检索或者过滤
        WaveEntry.Header.Builder headerBuilder = WaveEntry.Header.newBuilder();
        headerBuilder.setVersion(version);
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setServerenCode(CharsetUtils.DEFAULT_CHARSET);// 经过java输出后所有的编码为unicode
        headerBuilder.setExecuteTime(logHeader.getWhen() * 1000L);
//                headerBuilder.setSourceType(WaveEntry.Type.MYSQL); // default is MySQL
        if (logHeader.getGtid() != null && !logHeader.getGtid().equals("")) {
            WaveEntry.Pair.Builder pair = WaveEntry.Pair.newBuilder();
            pair.setKey("gtid").setValue(logHeader.getGtid());
            headerBuilder.addProps(pair);
        }
        if (eventType != null) {
            headerBuilder.setEventType(eventType);
        }
        if (db != null) {
            headerBuilder.setSchemaName(db);
        }
        if (table != null) {
            headerBuilder.setTableName(table);
        }
        headerBuilder.setEventLength(logHeader.getEventLen());
        return headerBuilder.build();
    }


    /**
     * 每条消息 作为一个单独的记录发送
     *
     * @param msg
     * @param id
     * @param generator
     * @param part
     * @param eventType
     */
    private void formatByOneRow(IMessage msg, String id,
                                IKeyGenerator generator,
                                IProducer producer,
                                IRepartition part,
                                WaveEntry.EventType eventType,
                                IBinlogWorker worker) {
        RowsLogEvent event = (RowsLogEvent) msg.getLogEvent();
        TableMapLogEvent table = event.getTable();
        TableMeta meta = msg.getTableMeta();

        int colNum = meta.getColumnSize();
        int rowNum = msg.getRowNumCounter();

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("row number " + rowNum + ", column number " + colNum);
        }

        List<String> columns = meta.columns; // 獲取列信息
        List<Boolean> isKey = meta.isKey;  // 獲取 列属性 是否为主键
        List<String> mySQLType = meta.mySQLType; // 获取列 属性 类型
        List<Integer> javaType = msg.getCJavaType(); // 获取列 属性 类型

        WaveEntry.Header header = createHeader(msg.getBinlogFile(),
                table.getDbName(), table.getTableName(),
                event.getHeader(), eventType);

        WaveEntry.RowChange.Builder rcb = WaveEntry.RowChange.newBuilder();
        rcb.setTableId(event.getTableId());
        rcb.setIsDdl(false);
        rcb.setEventType(eventType);

        // reserved column indexes
        BitSet rc = msg.getReservedCol();

        // 伪列信息
        List<WaveEntry.Column> fakeCols = getFakeColumns(msg, colNum);

        boolean isBefore = true;
        WaveEntry.RowData.Builder rdb = null;

        Boolean[] isUpdate = null;

        // packet number 包的个数
        int packetNum = 0;

        for (int i = 0; i < rowNum; i++) {
            // 获取行 列值
            String[] rowVal = msg.getRowVals(i);

            // 获取行 列值 null 标记
            Boolean[] isNull = msg.getIsNull(i);

            switch (eventType) {
                case UPDATE:
                    isUpdate = msg.getIsUpdated(i >> 1);
            }

            // 只有update 能够生成 new builder
            if (isBefore) {
                rdb = WaveEntry.RowData.newBuilder();
            }

            // row data build
            for (int j = 0; j < colNum; j++) {
                // 增加列
                WaveEntry.Column.Builder cb = WaveEntry.Column.newBuilder();
                cb.setName(columns.get(j)).
                        setIsKey(isKey.get(j)).
                        setMysqlType(mySQLType.get(j)).
                        setIndex(j).setSqlType(javaType.get(j));

                if (!rc.get(j) || isNull[j]) {
                    // 如果为过滤列名 或者 值为空
                    cb.setIsNull(true);
                } else {
                    cb.setIsNull(false).setValue(rowVal[j]);
                }

                switch (eventType) {
                    case DELETE:
                        // 增加列
                        rdb.addBeforeColumns(cb.build());
                        break;
                    case INSERT:
                        // 增加列
                        rdb.addAfterColumns(cb.build());
                        break;
                    case UPDATE:
                        if (isBefore) {
                            // 增加列
                            rdb.addBeforeColumns(cb.build());
                        } else {
                            cb.setUpdated(isUpdate[j]);
                            rdb.addAfterColumns(cb.build());
                        }
                        break;
                }

            }

            switch (eventType) {
                case DELETE:
                    // 增加列
                    rdb.addAllBeforeColumns(fakeCols);
                    break;
                case INSERT:
                    // 增加伪列
                    rdb.addAllAfterColumns(fakeCols);
                    break;
                case UPDATE:
                    if (isBefore) {
                        // 增加列
                        rdb.addAllBeforeColumns(fakeCols);
                        isBefore = false; // before columns 设置完成
                    } else {
                        isBefore = true; // 下一轮开始
                        rdb.addAllAfterColumns(fakeCols);
                    }
                    break;
            }

            if (isBefore) { // update 需要包含完整的消息体
                rcb.addRowDatas(rdb.build());

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("row data size " + rcb.getRowDatasCount() + "\n" + rcb);
                }

                WaveEntry.Entry entry = createEntry(header, WaveEntry.EntryType.ROWDATA, rcb.build().toByteString(), msg.getHost());

                packetNum++;

                if (packetNum != 1) { // likely
                    // 直接写入发送队列
                    part.offer(encapWithIncr(entry, generator, producer, msg, id, worker));
                } else {
                    // 直接写入发送队列
                    part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
                }
                // 创建新 entry
                rcb = WaveEntry.RowChange.newBuilder();
                rcb.setTableId(event.getTableId());
                rcb.setIsDdl(false);
                rcb.setEventType(eventType);
            }
        }
    }


    /**
     * 多条记录进行格式化
     *
     * @param msg
     * @param id
     * @param generator
     * @param producer
     * @param part
     * @param eventType
     */
    private void formatByRows(IMessage msg, String id,
                              IKeyGenerator generator,
                              IProducer producer,
                              IRepartition part,
                              WaveEntry.EventType eventType,
                              IBinlogWorker worker) {
        RowsLogEvent event = (RowsLogEvent) msg.getLogEvent();
        TableMapLogEvent table = event.getTable();
        TableMeta meta = msg.getTableMeta();

        int colNum = meta.getColumnSize();
        int rowNum = msg.getRowNumCounter();

        List<String> columns = meta.columns; // 獲取列信息
        List<Boolean> isKey = meta.isKey;  // 獲取 列属性 是否为主键
        List<String> mySQLType = meta.mySQLType; // 获取列 属性 类型
        List<Integer> javaType = msg.getCJavaType(); // 获取列 属性 类型

        if (javaType.size() < columns.size()) {
            colNum = javaType.size(); // 如果binlog 当中列数量 < 实际的列数量 应当以binlog 当中列数量为准
        }

        if (LogUtils.debug.isDebugEnabled() && javaType.size() < columns.size()) {
            LogUtils.debug.debug(meta.getFullName() +
                    "columns [" + columns + "], convert java type size [" + javaType.size() + "]");
        }

        WaveEntry.Header header = createHeader(msg.getBinlogFile(),
                table.getDbName(), table.getTableName(),
                event.getHeader(), eventType);

        WaveEntry.RowChange.Builder rcb = WaveEntry.RowChange.newBuilder();
        rcb.setTableId(event.getTableId());
        rcb.setIsDdl(false);
        rcb.setEventType(eventType);

        // reserved column indexes
        BitSet rc = msg.getReservedCol();

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("row number " + rowNum + ", column number " + colNum);
        }

        // 伪列信息
        List<WaveEntry.Column> fakeCols = getFakeColumns(msg, colNum);

        int packetSize = 0;
        boolean isBefore = true;
        WaveEntry.RowData.Builder rdb = null;
        Boolean[] isUpdated = null;

        int packetNum = 0; // 发送包的数量
        for (int i = 0; i < rowNum; i++) {
            // 获取行 列值
            String[] rowVal = msg.getRowVals(i);

            // 获取行 列值 null 标记
            Boolean[] isNull = msg.getIsNull(i);

            // 获取 字段是否更新
            if (eventType == WaveEntry.EventType.UPDATE) {
                isUpdated = msg.getIsUpdated(i >> 1);
            }

            if (isBefore) {
                // row data build
                rdb = WaveEntry.RowData.newBuilder();
            }

            for (int j = 0; j < colNum; j++) {
                // 增加列
                WaveEntry.Column.Builder cb = WaveEntry.Column.newBuilder();
                cb.setName(columns.get(j));
                cb.setIsKey(isKey.get(j));
                cb.setMysqlType(mySQLType.get(j));
                cb.setIndex(j);
                cb.setSqlType(javaType.get(j));
                if (!rc.get(j) || isNull[j]) {
                    // 如果为过滤列名称 或者为空值
                    cb.setIsNull(true);
                } else {
                    cb.setIsNull(false).setValue(rowVal[j]);
                    // 增加计算
                    packetSize += (columns.get(j).length() +
                            mySQLType.get(j).length() +
                            rowVal[j].length() + 4 + 1 + 4 + 1);
                }

                switch (eventType) {
                    case DELETE:
                        // 增加列
                        rdb.addBeforeColumns(cb.build());
                        break;
                    case INSERT:
                        // 增加列
                        rdb.addAfterColumns(cb.build());
                        break;
                    case UPDATE:
                        if (isBefore) {
                            // 增加列
                            rdb.addBeforeColumns(cb.build());
                        } else {
                            cb.setUpdated(isUpdated[j]);
                            rdb.addAfterColumns(cb.build());
                        }
                        break;
                }
            }

            switch (eventType) {
                case DELETE:
                    // 增加列
                    rdb.addAllBeforeColumns(fakeCols);
                    break;
                case INSERT:
                    // 增加伪列
                    rdb.addAllAfterColumns(fakeCols);
                    break;
                case UPDATE:
                    if (isBefore) {
                        // 增加列
                        rdb.addAllBeforeColumns(fakeCols);
                        isBefore = false; // before columns 设置完成
                    } else {
                        isBefore = true; // 下一轮开始
                        rdb.addAllAfterColumns(fakeCols);
                    }
                    break;
            }

            if (isBefore) {
                // 需要增加到行变更当中
                rcb.addRowDatas(rdb.build());
            }

            if (packetSize > MAX_PACKET_SIZE && isBefore) { // update 需要包含完整的消息体
                packetSize = 0;
                WaveEntry.Entry entry = createEntry(header, WaveEntry.EntryType.ROWDATA, rcb.build().toByteString(), msg.getHost());

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("row data size " + rcb.getRowDatasCount());
                }

                // 发送包的数量 + 1
                packetNum++;

                if (packetNum == 1) { // 至少会是1 likely
                    // 增加计数器
                    part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
                } else {
                    // 直接写入发送队列
                    part.offer(encapWithIncr(entry, generator, producer, msg, id, worker));
                }

                // 创建新 row change builder
                rcb = WaveEntry.RowChange.newBuilder();
                rcb.setTableId(event.getTableId());
                rcb.setIsDdl(false);
                rcb.setEventType(eventType);
            }
        }

        if (packetSize != 0) { // 还有剩余的包未发送 存在 rowChangeBuilder 当中
            WaveEntry.Entry entry = createEntry(header, WaveEntry.EntryType.ROWDATA, rcb.build().toByteString(), msg.getHost());

            if (LogUtils.debug.isDebugEnabled()) {
                LogUtils.debug.debug("debug row change build " + rcb);
            }

            // 发送包的数量 + 1
            packetNum++;

            if (packetNum != 1) { // 至少会是1
                // 增加计数器
                part.offer(encapWithIncr(entry, generator, producer, msg, id, worker));
            } else {
                // 直接写入发送队列
                part.offer(encapWithNoIncr(entry, generator, producer, msg, id, worker));
            }
        }
    }

    /**
     * 获取伪列 信息
     *
     * @param msg
     * @param colNum
     * @return
     */
    private List<WaveEntry.Column> getFakeColumns(IMessage msg, int colNum) {
        ArrayList<WaveEntry.Column> fakeCols = new ArrayList<WaveEntry.Column>(4);
        for (Map.Entry<String, String> entry : msg.getFakeCols().entrySet()) {
            WaveEntry.Column.Builder cb = WaveEntry.Column.newBuilder();
            cb.setName(entry.getKey()).
                    setIsKey(false).
                    setMysqlType("varchar(255)").
                    setIndex(colNum++).setIsNull(false).
                    setValue(entry.getValue()).setSqlType(Types.VARCHAR);
            fakeCols.add(cb.build());
        }
        return fakeCols;
    }
}
