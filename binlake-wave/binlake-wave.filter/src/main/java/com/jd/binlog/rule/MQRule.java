package com.jd.binlog.rule;

import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.LogPosition;
import com.jd.binlog.dbsync.event.GtidLogEvent;
import com.jd.binlog.dbsync.event.QueryLogEvent;
import com.jd.binlog.filter.MQFilter;
import com.jd.binlog.inter.filter.IFilter;
import com.jd.binlog.inter.msg.IConvert;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.msg.IRepartition;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IKeyGenerator;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.performance.PerformanceUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 18-5-14
 * <p>
 * MQ 规则
 *
 * @author pengan
 */
public class MQRule implements IRule {
    AtomicBoolean isClosed = new AtomicBoolean(false);

    List<IFilter> white = new ArrayList<>(4);
    List<IFilter> black = new ArrayList<>(4);

    boolean withTrx;
    /**
     * 优先级指标 消息发送条数
     */
    long priority;

    String topic;
    Meta.OrderType order;
    IBinlogWorker worker;
    IKeyGenerator generator;
    IRepartition part;
    IConvert call;
    IProducer producer;

    /**
     * 回调接口
     *
     * @param rule
     * @param convert
     * @param producer
     */
    public MQRule(Meta.MQRule rule,
                  IConvert convert,
                  IProducer producer,
                  IRepartition part,
                  IBinlogWorker worker) {
        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("mq rule " + rule);
        }

        this.topic = rule.getTopic();
        this.withTrx = rule.isWithTransaction();
        this.order = rule.getOrder();
        this.worker = worker;
        switch (order) {
            case NO_ORDER:
                generator = new IKeyGenerator.RandOrder();
                break;
            case BUSINESS_KEY_ORDER:
                generator = new IKeyGenerator.BusinessOrder();
                break;
            case TRANSACTION_ORDER:
                generator = new IKeyGenerator.TransactionOrder();
                break;
            case TABLE_ORDER:
                generator = new IKeyGenerator.TableOrder();
                break;
            case DB_ORDER:
                generator = new IKeyGenerator.DbOrder();
                break;
            case INSTANCE_ORDER:
                generator = new IKeyGenerator.InstanceOrder();
                break;
        }

        this.call = convert;
        this.producer = producer;
        this.part = part;

        for (Meta.Filter filter : rule.getWhite()) {
            white.add(new MQFilter(filter, true));
        }

        for (Meta.Filter filter : rule.getBlack()) {
            black.add(new MQFilter(filter, false));
        }
    }


    @Override
    public void convert(IMessage msg) {
        LogUtils.debug.debug("convert");

        IBinlogWorker worker = this.worker;
        IKeyGenerator generator = this.generator;
        IRepartition part = this.part;
        IConvert call = this.call;
        IProducer producer = this.producer;
        if (worker == null ||
                generator == null ||
                part == null ||
                call == null ||
                producer == null) {
            LogUtils.warn.warn("dump thread is already closed rule closed too!!!!!");
            return;
        }

        // 正常来说如果这里出现异常 说明规则或者格式转换出现了异常 则 convert 或者 过滤规则当中有 漏洞
        try {
            LogPosition logPos = msg.getLogPosition();

            if (LogUtils.debug.isDebugEnabled()) {
                LogUtils.debug.debug("before log position in message is ==" + logPos + "===");
            }

            boolean isNotCommit = !logPos.isCommit(); // 到这儿 log position 肯定不为空

            boolean isPenetrate = formatMessage(msg, worker, generator, part, call, producer);

            if (isPenetrate && isNotCommit) {

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("after log position in message is ==" + logPos + "===");
                }

                // remove log position
                worker.removeLogPosition(logPos);
            }
        } catch (Exception exp) {
            LogUtils.error.error("convert error", exp);

            // 关闭worker 清空所有占用资源
            worker.close();
        }
    }

    /**
     * @param msg
     * @param worker
     * @param generator
     * @param part
     * @param call
     * @param producer
     * @return {penetrate :true } 说明消息已经被过滤掉 或者 并未发送 {列过滤}
     */
    private boolean formatMessage(IMessage msg, IBinlogWorker worker,
                                  IKeyGenerator generator, IRepartition part,
                                  IConvert call, IProducer producer) {
        int eventType = msg.getEventType();
        String table = msg.getTable();
        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("formatMessage event type " + eventType + " table " + table);
        }

        try {
            List<IFilter> match = new ArrayList<>(4);
            /**
             * 优先筛选白名单
             */
            boolean resFlag = false; // 初始化 全部过滤掉
            for (IFilter filter : white) {
                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("white filter table " + ((table == null) ? "null" : table) + ", event type " + eventType);
                }

                if (!filter.filterEventType(eventType) || (table != null && !filter.filterTable(table))) {
                    // 不在白名单事件类型当中 或者 表名不在白名单当中
                    continue;
                }
                resFlag = true;
                match.add(filter);
            }

            if (!resFlag) { // 白名单取并集
                return true; // 直接过滤掉事件
            }

            /**
             * 筛选黑名单 如果有一个命中过滤器 则直接过滤
             */
            for (IFilter filter : black) {
                if (filter.filterEventType(eventType) && (table != null && filter.filterTable(table))) {
                    // 在黑名单事件类型当中 且 在黑名单列表当中
                    return true;
                }
            }
            // 有时候begin 或者 commit table是为空 即使match size == 0

            /**
             * 满足行数据类型 需要开启列过滤规则
             */
            switch (eventType) {
                case LogEvent.WRITE_ROWS_EVENT_V1:
                case LogEvent.WRITE_ROWS_EVENT:
                    if (handleMsgColumn(call, match, msg)) {
                        return true;
                    }

                    // 根据具体的消息格式组装成具体的消息形式
                    switch (order) {
                        case BUSINESS_KEY_ORDER: // 必须要按行来发送消息
                            call.formatInsertByOneRow(msg, topic, generator, part, producer, worker);
                            break;
                        default:
                            call.formatInsertByRows(msg, topic, generator, part, producer, worker);
                    }
                    priority++;
                    break;
                case LogEvent.UPDATE_ROWS_EVENT_V1:
                case LogEvent.UPDATE_ROWS_EVENT:
                    if (handleMsgColumn(call, match, msg)) {
                        return true;
                    }

                    // 根据具体的消息格式组装成具体的消息形式
                    switch (order) {
                        case BUSINESS_KEY_ORDER:
                            call.formatUpdateByOneRow(msg, topic, generator, part, producer, worker);
                            break;
                        default:
                            call.formatUpdateByRows(msg, topic, generator, part, producer, worker);
                    }
                    priority++;
                    break;
                case LogEvent.DELETE_ROWS_EVENT_V1:
                case LogEvent.DELETE_ROWS_EVENT:
                    if (handleMsgColumn(call, match, msg)) {
                        return true;
                    }

                    // 根据具体的消息格式组装成具体的消息形式
                    switch (order) {
                        case BUSINESS_KEY_ORDER:
                            call.formatDeleteByOneRow(msg, topic, generator, part, producer, worker);
                            break;
                        default:
                            call.formatDeleteByRows(msg, topic, generator, part, producer, worker);
                    }
                    priority++;
                    break;
                case LogEvent.QUERY_EVENT:
                    QueryLogEvent qle = (QueryLogEvent) msg.getLogEvent();
                    String query = qle.getQuery();

                    if (LogUtils.debug.isDebugEnabled()) {
                        LogUtils.debug.debug("query is ==" + query + "==");
                    }

                    if (StringUtils.equals(query, IConvert.BEGIN)) {
                        if (withTrx) { // 是否过滤事务 begin /commit
                            call.formatBeginQuery(msg, topic, generator, part, producer, worker);
                            priority++;
                        } else {
                            return true; // 消息过滤掉
                        }
                        break;
                    }

                    if (StringUtils.equals(query, IConvert.COMMIT)) {
                        if (withTrx) { // 是否过滤事务 begin /commit
                            call.formatCommitQuery(msg, topic, generator, part, producer, worker);
                            priority++;
                        } else {
                            return true; // 消息过滤掉
                        }
                        break;
                    }
                    call.formatDDLQuery(msg, topic, generator, part, producer, worker);
                    priority++;
                    break;
                case LogEvent.XID_EVENT:
                    if (withTrx) { // 携带事务标记
                        call.formatCommitXID(msg, topic, generator, part, producer, worker);
                        priority++;
                        break;
                    } else {
                        return true; // 消息过滤掉
                    }
                case LogEvent.ROWS_QUERY_LOG_EVENT:
                    call.formatRowsQuery(msg, topic, generator, part, producer, worker);
                    priority++;
                    break;
                case LogEvent.USER_VAR_EVENT:
                    call.formatUserVar(msg, topic, generator, part, producer, worker);
                    priority++;
                    break;
                case LogEvent.INTVAR_EVENT:
                    call.formatIntVar(msg, topic, generator, part, producer, worker);
                    priority++;
                    break;
                case LogEvent.RAND_EVENT:
                    call.formatRand(msg, topic, generator, part, producer, worker);
                    priority++;
                    break;
                case LogEvent.GTID_LOG_EVENT:
                    GtidLogEvent event = (GtidLogEvent) msg.getLogEvent();
                    msg.setGTID(event.getSid(), event.getGtidGNO());
                    return true;
                default:
                    return true; // 消息过滤掉
            }
        } finally {
            PerformanceUtils.perform(PerformanceUtils.CONVERT_DELAY_KEY, msg.getLogEvent().getWhen());
            // 去除所有引用
            msg.clearReference();
        }

        return false;
    }

    @Override
    public long priority() {
        return priority;
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }

        this.worker = null;
        this.generator = null;
        this.part = null;
        this.call = null;
        IProducer producer = this.producer;
        if (producer != null) {
            producer.close();
        }
        this.producer = null;
    }

    /**
     * 处理消息列 字段
     *
     * @param match
     * @param msg
     * @return {true} all columns are filtered
     */
    private boolean handleMsgColumn(IConvert call, List<IFilter> match, IMessage msg) {
        if (match.size() == 0) { // 未匹配上任何过滤规则
            return true;
        }

        // TODO: 18-5-14  过滤列数据
        List<String> cols = msg.getColumns();
        List<String> upCols = msg.getUpperColumns();
        int len = cols.size();

        Set<Integer> filteredCols = new HashSet<Integer>(); // 过滤的列集合

        for (IFilter filter : match) {
            // reserved columns 保留列
            BitSet rc = new BitSet(len);
            rc.set(0, len); // 如果没有过滤规则 则使用所有的列

            // 确定过滤列 位图
            if (filter.haveColumnFilter()) {
                for (int i = 0; i < len; i++) {
                    if (filter.filterColumn(cols.get(i))) {
                        filteredCols.add(i); // 放入 过滤列
                        rc.set(i, false);
                    }
                }
            }

            // 确定业务主键 位图
            if (filter.haveHashKeys()) {
                BitSet keys = new BitSet(len);
                for (int i = 0; i < len; i++) {
                    if (filter.getHashKeys().contains(upCols.get(i))) {
                        keys.set(i, true); // 标识业务主键位置
                    }
                }
                msg.applyKeyBitSet(keys);
            }

            LogUtils.debug.debug(new Runnable() {
                @Override
                public void run() {
                    StringBuilder kbs = new StringBuilder();
                    for (int i = 0; i < len; i++) {
                        kbs.append("index ").append(i).append(" : ").append(msg.getKeyBitSet().get(i)).append(",");
                    }
                    LogUtils.debug.debug("key bit set " + kbs);
                }
            });

            // 列名 已经匹配上 现在需要添加 伪列 信息
            call.appendFakeColumns(filter, rc, msg);
        }

        return filteredCols.size() == len;
    }
}
