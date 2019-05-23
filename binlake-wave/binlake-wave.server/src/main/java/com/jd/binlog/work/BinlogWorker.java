package com.jd.binlog.work;

import com.jd.binlog.alarm.RetryTimesAlarmUtils;
import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.convert.Converter;
import com.jd.binlog.convert.TableRowsParser;
import com.jd.binlog.dbsync.*;
import com.jd.binlog.dbsync.event.*;
import com.jd.binlog.dump.BinlogDump;
import com.jd.binlog.dump.DumpType;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.filter.aviater.AviaterRegexFilter;
import com.jd.binlog.inter.msg.IConvert;
import com.jd.binlog.inter.msg.IRepartition;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IBinlogHandler;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.mysql.EOFPacket;
import com.jd.binlog.mysql.ErrorPacket;
import com.jd.binlog.net.BufferPool;
import com.jd.binlog.parser.SimpleDdlParser;
import com.jd.binlog.parser.TableMeta;
import com.jd.binlog.parser.TableMetaCache;
import com.jd.binlog.performance.PerformanceUtils;
import com.jd.binlog.producer.Producer;
import com.jd.binlog.protocol.WaveEntry;
import com.jd.binlog.rule.MQRule;
import com.jd.binlog.util.CharUtils;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.util.TimeUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import static com.jd.binlog.inter.msg.IConvert.BEGIN;
import static com.jd.binlog.inter.msg.IConvert.COMMIT;
import static com.jd.binlog.util.GTIDUtils.compare;

/**
 * Created on 18-5-12
 *
 * @author pengan
 */
public class BinlogWorker extends Thread implements IBinlogWorker {
    // 所有的发送规则是 乱序
    private boolean isMess = false;

    // read buffer offset
    private volatile int offset = 0;

    // last read buffer that not end with 1 package
    private ByteBuffer lastBuffer;

    // binlog file only exist in rotate event so have
    private volatile String binlogFile = "";

    // is terminal gtid
    private volatile boolean isTerminalGtid;

    // is closed atomic boolean flag to mark the MySQL connection dump is closed
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    // 操作 log position 队列锁
    private ReentrantLock lock = new ReentrantLock();

    // 由于有限流 所以队列长度不会太大 最大长度 processors + bufferSize
    private ConcurrentLinkedQueue<LogPosition> logPositions = new ConcurrentLinkedQueue<LogPosition>();

    // gtids
    private Map<String, GTID> gtids = new HashMap<String, GTID>();

    // 过滤 pt工具表生成器
    private AviaterRegexFilter ptOnlineTableFilter = new AviaterRegexFilter("^_.*_old$,^_.*_new$");

    // dump byte buffer pool 传入参数
    private BufferPool bufferPool;

    // MySQL meta info 传入参数
    private MetaInfo metaInfo;

    // dump MySQL 实例 host:port
    private String host;

    // transaction id
    private long trxID = 0;

    // monitor map
    private Map<String, Object> monitor;

    // MySQL dump connection
    private MySQLConnector dump;

    // Binlog decoder
    private LogDecoder decoder; // init

    // Binlog Context
    private LogContext context; // init

    // leader selector 循环引用 无法造成fgc 传入参数
    private ILeaderSelector leader;

    // TableMeta cache contains table meta information {field type and field value}
    // 原生 binlog当中的表名 以及库名 不要更新大小写
    private TableMetaCache tableMetaCache;

    // 初始化的log position
    private LogPosition originPos;

    // rule list
    private List<IRule> rs;

    // rule number
    private int ruleNum;

    // partition function
    private IRepartition part;

    // binlog handler for filter data and send message
    private IBinlogHandler handler;

    // throttler queue for binlog dump 阻流器
    private LinkedBlockingQueue<Object> throttler = new LinkedBlockingQueue<Object>();

    /**
     * 启动binlog worker
     *
     * @param partition
     * @param throttleSize 流控 buffer 大小
     * @param bufferPool
     * @param metaInfo
     * @param leader
     * @throws Exception
     */
    public BinlogWorker(int partition,
                        int throttleSize,
                        BufferPool bufferPool,
                        MetaInfo metaInfo,
                        ILeaderSelector leader,
                        ArrayList<LinkedBlockingQueue<IRule.RetValue>> queues) throws Exception {
        super(metaInfo.getHost());

        // 每个事件可能都需要使用 所以初始化1次
        this.leader = leader;
        this.metaInfo = metaInfo;
        this.bufferPool = bufferPool;
        this.host = metaInfo.getHost() + ":" + metaInfo.getPort();

        // init part
        this.part = initPartition(partition, queues);

        this.rs = new ArrayList<IRule>();
        for (Meta.Rule rule : metaInfo.getDbInfo().getRule()) {
            switch (rule.getStorage()) {
                case KV_STORAGE:
                    break;
                case MQ_STORAGE:
                    isMess &= initMQRule(rule);
                    break;
            }
        }
        // 常用 不再重复计算
        this.ruleNum = rs.size();

        if (this.ruleNum == 0) {
            // 如果规则不存在 则直接放弃dump 数据抛出异常 进行重试
            throw new BinlogException(ErrorCode.ERR_RULE_EMPTY, new Exception("过滤规则为空"));
        }

        fillThrottler(throttleSize);

        // MySQL connector 管理端已经验证 正常来说应该能够支持
        this.dump = new MySQLConnector(metaInfo.getDbInfo());

        // log position
        this.logPositions = new ConcurrentLinkedQueue<>();

        // table meta cache using guava cache
        this.tableMetaCache = new TableMetaCache(new MySQLConnector(metaInfo.getDbInfo()));

        // 分配内存
        this.lastBuffer = bufferPool.allocate();

        this.handler = new BinlogHandler(rs, host, isMess, this);

        // init monitor for capacity 4 size
        this.monitor = new HashMap<>(4);

        // remember the current timestamp
        monitor.put("start", TimeUtils.time());
    }

    /**
     * 填充 流控器
     *
     * @param throttleSize
     */
    private void fillThrottler(int throttleSize) {
        for (int i = 0; i < throttleSize; i++) {
            throttler.offer(object);
        }
    }


    /**
     * 初始化 mq 规则
     *
     * @param rule
     * @return isMess 是否是乱序 很有可能出现异常  所以提前
     * @throws Exception
     */
    private boolean initMQRule(Meta.Rule rule) throws Exception {
        Meta.MQRule mqRule = Meta.MQRule.unmarshalJson(rule.getAny());

        // producer implement
        IProducer producer = Producer.initProducer(mqRule.getProducerClass(), mqRule.getPara(), mqRule.getTopic());

        // convert implement
        IConvert conv = Converter.initConverter(rule.getConvertClass());

        MQRule mr = new MQRule(mqRule, conv, producer, part, this);
        rs.add(mr);

        return mqRule.getType() == Meta.OrderType.NO_ORDER;
    }

    /**
     * 依据partition 初始化 queue
     *
     * @param partition
     * @param queues
     * @return IRepartition
     */
    private IRepartition initPartition(int partition,
                                       final List<LinkedBlockingQueue<IRule.RetValue>> queues) {
        LogUtils.info.info("queue number is " + partition);

        return retValue -> {
            // never will the retValue is null
            queues.get(retValue.partition % partition).offer(retValue);
        };
    }

    @Override
    public void run() {
        try {
            // prepare
            prepare();

            // read from socket channel
            read();
        } catch (InterruptedException e) {
            // InterruptedException that means all have closed because of the interrupt
            close(); // just close
        } catch (BinlogException e) {
            handleException(e);
        } catch (Throwable e) {
            handleException(new BinlogException(ErrorCode.ERR_UNKNOWN, e));
        }
    }

    @Override
    public boolean close() {
        if (!isClosed.compareAndSet(false, true)) {
            LogUtils.debug.warn("already closed");
            return false;
        }
        LogUtils.info.info("close handler");

        long connectionId = dump.getConnectionId();
        LogUtils.info.info(host + " close MySQL connection " + connectionId);

        try {
            dump.disconnect();
            dump = null;
        } catch (IOException e) {
            LogUtils.error.error("MySQL " + host + " connect error ", e);
        }

        // using the already connected Connection
        try {
            if (connectionId != 0) {
                tableMetaCache.kill(connectionId);
            }
        } catch (Throwable exp) {
            LogUtils.error.error("close " + host + " connection error ", exp);
        }

        LogUtils.info.info("close table meta cache");
        try {
            tableMetaCache.close();
            tableMetaCache = null;
        } catch (Throwable exp) {
            LogUtils.error.error("close " + host + " table meta cache error ", exp);
        }

        LogUtils.info.info("close handler thread");
        // close handler
        if (handler != null) {
            handler.clear();
        }
        handler = null;

        // 清空流控器
        throttler.clear();
        throttler = null;

        // 置空 去除悬挂引用 防止gc 失败问题 需要重新获取数据
        logPositions.clear();
        logPositions = null;

        gtids.clear();
        gtids = null;

        ptOnlineTableFilter = null;

        bufferPool.recycle(lastBuffer);
        lastBuffer = null;
        bufferPool = null;

        decoder = null;
        context.clearAllTables();
        context = null;

        originPos.clear();
        originPos = null;

        for (IRule rule : rs) {
            // 关闭 规则
            rule.close();
        }
        rs.clear();
        rs = null;

        part = null;

        monitor.clear();
        monitor = null;

        try {
            /**
             * 当前线程 不是发起close线程
             */
            this.interrupt();
        } catch (Throwable exp) {
            LogUtils.error.error("binlog worker interrupt", exp);
        }
        return true;
    }

    @Override
    public boolean isClosed() {
        return isClosed.get();
    }

    /**
     * 打印binlog 位置日志
     *
     * @param logPositions
     */
    private void debugLogPosition(ConcurrentLinkedQueue<LogPosition> logPositions) {
        if (LogUtils.debug.isDebugEnabled()) {
            Iterator<LogPosition> liter = logPositions.iterator();
            boolean isHead = true;
            int count = 0;
            while (liter.hasNext()) {
                LogPosition lp = liter.next();
                if (isHead) {
                    LogUtils.debug.debug(host + " truncLogPosQueue logPositions head is " + lp);
                    isHead = false;
                }
                count++;
            }
            LogUtils.debug.debug(host + " truncLogPosQueue logPositions queue size " + count);

            BlockingQueue<Object> queue = this.throttler;
            if (queue != null) {
                LogUtils.debug.debug(host + " throttler queue size " + queue.size());
            }
        }
    }

    /**
     * 合并commit 位置
     * ----------------------|    |--------------------|    |--------------------|
     * node1.isCommit = true | -> | node2.isCommit=true| -> |node3.isCommit=false| ...
     * ----------------------|    |--------------------|    |--------------------|
     * <p>
     * then the result remove 1node leave 2node... behind
     */
    private void truncLogPosQueue(ConcurrentLinkedQueue<LogPosition> logPositions) {
        if (logPositions.isEmpty()) {
            LogUtils.warn.warn("no binlog position object in queue");
            return;
        }

        // 根据concurrent list 实现方式 一次size 相当于 直接遍历一遍 链表
        LogPosition curr = null;
        LogPosition pre = null;

        LinkedList<LogPosition> rms = new LinkedList<LogPosition>();
        Iterator<LogPosition> iterator = logPositions.iterator();

        while (iterator.hasNext()) {
            if (pre == null) {
                pre = iterator.next();
                continue;
            }
            curr = iterator.next();

            if (pre.isCommit() && curr.isCommit()) {
                rms.add(pre);
            }
            pre = curr;
        }

        removeQueueWithLock(logPositions, rms);

        // 轻易不要开work日志
        debugLogPosition(logPositions);
    }

    /**
     * 如果未获取到锁 可能有冲突 立马返回 等待下次执行
     *
     * @param logPositions binlog 位置队列 {线程安全}
     * @param rms          需要被删除的 对象
     */
    private void removeQueueWithLock(ConcurrentLinkedQueue<LogPosition> logPositions,
                                     List<LogPosition> rms) {
        if (lock.tryLock()) {
            try {
                // 删除队列 需要有锁防止写冲突 注意这里的log pos 与 work.removeLogPosition() 为不同属性
                for (LogPosition lp : rms) {
                    logPositions.remove(lp);
                    keepDump();
                }
            } finally {
                lock.unlock();
            }
        }
        rms.clear();
    }

    @Override
    public LogPosition getLatestLogPosWithRm() {
        ConcurrentLinkedQueue<LogPosition> logPositions = this.logPositions;
        if (logPositions == null) {
            return null;
        }

        // 轻易不要开work日志
        debugLogPosition(logPositions);

        LogPosition curr;
        LogPosition pre = null;
        int len = 0;

        List<LogPosition> rms = new LinkedList<>();
        // 避免进入无线循环当中 所以控制次数  不需要担心队列过长 因为有truncate Log position 保证
        Iterator<LogPosition> iter = logPositions.iterator();
        while (iter.hasNext() && (curr = iter.next()) != null
                && len++ < THROTTLE_QUEUE_SIZE) {
            if (curr.isCommit()) {
                rms.add(curr); // 添加到 删除队列
                pre = curr;
                continue;
            }
            break; // 如果不是commit 直接退出
        }

        removeQueueWithLock(logPositions, rms);

        if (pre != null) {
            pre.mergeOriginLogPos(originPos);
            pre.refresh();
            return pre;
        }
        return null;
    }

    @Override
    public LogPosition getLatestLogPos() {
        ConcurrentLinkedQueue<LogPosition> logPositions = this.logPositions;
        if (logPositions == null) {
            return null;
        }

        debugLogPosition(logPositions);

        LogPosition curr;
        LogPosition pre = null;
        int len = 0;

        // 避免进入无线循环当中 所以控制次数  不需要担心队列过长 因为有truncate Log position 保证
        Iterator<LogPosition> iter = logPositions.iterator();
        while (iter.hasNext() && (curr = iter.next()) != null
                && len++ < THROTTLE_QUEUE_SIZE) {
            if (curr.isCommit()) {
                pre = curr;
                continue;
            }
            break; // 如果不是commit 直接退出
        }

        if (pre != null) {
            LogPosition np = pre.clone();
            np.mergeOriginLogPos(originPos);
            np.refresh();
            return np;
        }
        return null;
    }

    @Override
    public void handleException(BinlogException exp) {
        LogUtils.debug.debug("handleException stop dump service");

        LogUtils.error.error("host: " + host + " io exception ", exp);

        boolean success = false;

        // 获取最新的binlog位置 在io异常情况下 将这个位置同步到zookeeper
        try {
            leader.refreshLogPos();
        } catch (Exception e) {
            LogUtils.error.error("refresh log position error ", e);
        }

        try {
            success = close();
        } catch (Throwable e) {
            LogUtils.error.error("close session error", e);
        }

        // increase retryTimes meta and push to zookeeper
        ILeaderSelector leader = this.leader;
        MetaInfo metaInfo = this.metaInfo;
        if (success && leader != null && metaInfo != null) {
            // get meta info
            Meta.Error err = new Meta.Error().
                    setCode(exp.getErrorCode().errorCode).
                    setMsg(exp.message(metaInfo.getBinlogInfo().getLeader(), host));

            LogUtils.warn.warn("add session retry times");
            switch (exp.getErrorCode().according()) {
                case Retry:
                    metaInfo.addSessionRetryTimes();
                    metaInfo.setError(err);
                    break;
                case Stop:
                    metaInfo.fillRetryTimes();
                    metaInfo.setError(err);
            }

            RetryTimesAlarmUtils.alarm(metaInfo.getRetryTimes(),
                    "MySQL instance:" + metaInfo.getHost() + ":" + metaInfo.getPort()
                            + ", with retry times " + metaInfo.getRetryTimes() + ", with pre-leader "
                            + metaInfo.getBinlogInfo().getPreLeader() + ", current wave host " + host);

            try {
                leader.updateCounter(metaInfo);
            } catch (Exception e) {
                LogUtils.error.error("update counter error", e);
            }

            LogUtils.warn.warn("abandon leader ship");
            leader.abandonLeaderShip();

            // only close success then have the lock to reset null
            this.leader = null;
            this.metaInfo = null;
        }
    }

    @Override
    public void handleError(Throwable exp) {
        LogUtils.error.error("host: " + host + " error ", exp);
        close();
    }

    @Override
    public void removeLogPosition(LogPosition target) {
        ConcurrentLinkedQueue<LogPosition> logPositions = this.logPositions;
        if (logPositions == null) {
            return;
        }

        // 判断引用基数是否为 0
        int counter = target.decrementAndGet();

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("quote counter is " + counter + ", log position " + target);
        }

        if (counter == 0) {
            logPositions.remove(target);
            // 如果计数器 != 0 则不能往 阻流器 当中填充object 因为有可能会导致队列无限大 1 -> 16 
            keepDump();
        }
    }

    public void read() throws IOException, InterruptedException {
        ByteBuffer buffer;

        SocketChannel channel = dump.getChannel();

        // read bytes from channel into buffer
        while (!isClosed() && !isInterrupted() &&
                (buffer = this.lastBuffer) != null) { // continue when queue have at least 1 object or else will block

            int got = channel.read(buffer);
            if (got < 0) {
                LogUtils.error.error(host + " " + " get < 0 !!!");
                throw new IOException("got < 0");
            }

            TableMetaCache meta = this.tableMetaCache;
            ILeaderSelector leader = this.leader;
            Map<String, GTID> gtids = this.gtids;
            ConcurrentLinkedQueue<LogPosition> logPositions = this.logPositions;
            LogDecoder decoder = this.decoder;
            LinkedBlockingQueue<Object> throttler = this.throttler;
            if (meta == null
                    || leader == null
                    || gtids == null
                    || logPositions == null
                    || decoder == null
                    || throttler == null) {
                return;
            }
            // prevent NullPointerException
            handlePackage(buffer, meta, leader, gtids, logPositions, decoder, throttler);
        }
    }

    @Override
    public void startWorking() {
        this.start(); // thread start
        this.handler.startHandler(); // startWorking handler thread
    }

    /**
     * open the valve to keep on dump data
     */
    public void keepDump() {
        LinkedBlockingQueue<Object> thq = throttler;
        if (thq != null) {
            thq.offer(object);
        }
    }

    @Override
    public Map<String, Object> monitor() {
        return this.monitor;
    }

    /**
     * handle byte buffer packet
     *
     * @param buffer
     * @param meta
     * @param leader
     * @param gtids
     * @param logPositions
     * @param decoder
     */
    private final void handlePackage(ByteBuffer buffer,
                                     TableMetaCache meta,
                                     ILeaderSelector leader,
                                     Map<String, GTID> gtids,
                                     ConcurrentLinkedQueue<LogPosition> logPositions,
                                     LogDecoder decoder,
                                     LinkedBlockingQueue<Object> throttler) throws InterruptedException {
        int offset = this.offset, position = buffer.position(), length;

        while (!isClosed()) {
            length = getPacketLength(buffer, offset);

            if (length == -1) {// 未达到可计算数据包长度的数据
                if (!buffer.hasRemaining()) {
                    checkReadBuffer(buffer, offset, position);
                }
                break;
            }

            if (position >= offset + length) {
                // 提取一个数据包的数据进行处理
                buffer.position(offset);
                byte[] data = new byte[length];

                buffer.get(data, 0, length);
                decodeBytes(data, meta, leader, gtids, logPositions, decoder, throttler);

                // 设置偏移量
                offset += length;

                if (position == offset) {// 数据正好全部处理完毕
                    if (this.offset != 0) {
                        this.offset = 0;
                    }
                    buffer.clear();
                    break;
                } else {// 还有剩余数据未处理
                    this.offset = offset;
                    buffer.position(position);
                    continue;
                }
            } else {// 未到达一个数据包的数据
                if (!buffer.hasRemaining()) {
                    checkReadBuffer(buffer, offset, position);
                }
                break;
            }
        }
    }

    /**
     * get packet length using byte buffer
     *
     * @param buffer
     * @param offset
     * @return
     */
    private final int getPacketLength(ByteBuffer buffer, int offset) {
        if (buffer.position() < offset + PACKET_HEAD_SIZE) {
            return -1;
        } else {
            int length = buffer.get(offset) & 0xff;
            length |= (buffer.get(++offset) & 0xff) << 8;
            length |= (buffer.get(++offset) & 0xff) << 16;
            return length + PACKET_HEAD_SIZE;
        }
    }

    /**
     * 检查ReadBuffer容量，不够则扩展当前缓存，直到最大值。
     */
    private final ByteBuffer checkReadBuffer(ByteBuffer buffer, int offset, int position) {
        // 当偏移量为0时需要扩容，否则移动数据至偏移量为0的位置。
        if (offset == 0) {
            if (buffer.capacity() >= MAX_PACKET_SIZE) {
                throw new IllegalArgumentException("Packet size over the limit.");
            }

            LogUtils.warn.warn("checkReadBuffer==2, offset: " + offset + ", position: " + position + ", remaining: " + buffer.remaining());

            int size = buffer.capacity() << 1;
            size = (size > MAX_PACKET_SIZE) ? MAX_PACKET_SIZE : size;
            ByteBuffer newBuffer = ByteBuffer.allocate(size);
            buffer.position(offset);
            newBuffer.put(buffer);
            this.lastBuffer = newBuffer;

            // 回收扩容前的缓存块
            bufferPool.recycle(buffer);

            return newBuffer;
        } else {

            buffer.position(offset);
            buffer.compact();
            this.offset = 0;
            return buffer;
        }
    }

    /**
     * here packet is get success
     *
     * @param data
     * @param meta
     * @param leader
     * @param gtids
     * @param logPositions
     * @param decoder
     * @param throttler
     * @throws InterruptedException
     */
    private final void decodeBytes(byte[] data,
                                   TableMetaCache meta, ILeaderSelector leader,
                                   Map<String, GTID> gtids,
                                   ConcurrentLinkedQueue<LogPosition> logPositions,
                                   LogDecoder decoder,
                                   LinkedBlockingQueue<Object> throttler) throws BinlogException, InterruptedException {
        throttler.take(); // 从阻流器当中取出一个

        long dumpTime = TimeUtils.time();
        LogEvent event;
        switch (data[4]) {
            case EOFPacket.FIELD_COUNT:
                // just an eof no need to worry
                LogUtils.debug.debug("eof read from master");

                keepDump();
                // offer object to throttler
                return;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(data, 4);
                throw new BinlogException(ErrorCode.valueOfMySQLErrno(err.errno), new Exception(new String(err.message)), binlogFile + ":4");
            default:
                LogUtils.debug.debug("continue");
        }

        int origin = PACKET_HEAD_SIZE + 1;
        int limit = data.length - origin;
        LogBuffer buffer = new LogBuffer(data, origin, limit);
        event = decoder.decode(buffer, context);

        if (event == null) {
            LogUtils.warn.warn("event is null");

            keepDump();
            return;
        }

        // calculate dump delay
        this.monitor.put(PerformanceUtils.DUMP_DELAY_KEY, event.getWhen() - dumpTime);

        // 日志记录当前延迟时间
        PerformanceUtils.perform(PerformanceUtils.DUMP_DELAY_KEY, event.getWhen(), dumpTime);
        PerformanceUtils.perform(PerformanceUtils.DECODE_DELAY_KEY, event.getWhen());

        Carrier carrier = new Carrier(binlogFile, host, event);

        truncLogPosQueue(logPositions);
        try {
            // 阻塞以免避免任务队列过长
            boolean callKeepDump = handle(event, carrier, meta, leader, gtids, logPositions);
            if (callKeepDump) {
                keepDump();
            }
        } catch (BinlogException e) {
            handleException(e);
        } catch (Throwable e) {
            handleError(e);
        }
    }


    /**
     * handle event which data is stored into carrier 根据某些字段 并行
     *
     * @param event
     * @param carrier
     * @param meta
     * @param leader
     * @param gtids
     * @param logPositions @throws Exception
     * @return boolean whether to call keepDump
     */
    private boolean handle(LogEvent event, Carrier carrier, TableMetaCache meta,
                           ILeaderSelector leader, Map<String, GTID> gtids,
                           ConcurrentLinkedQueue<LogPosition> logPositions) throws BinlogException {
        int eventType = event.getHeader().getType();
        boolean isCommit = false;

        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                // 更新当前event的binlog file
                binlogFile = ((RotateLogEvent) event).getFilename(); // 只需要更新当前binlog file name
                return true;
            case LogEvent.QUERY_EVENT:
                // ddl语句 可能需要更新元数据信息 刷新指定 tableMeta
                isCommit = updateTableMetaByDDL((QueryLogEvent) event, meta, carrier);
                break;
            case LogEvent.USER_VAR_EVENT:
            case LogEvent.INTVAR_EVENT:
            case LogEvent.RAND_EVENT:
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                TableMapLogEvent tme = ((RowsLogEvent) event).getTable();

                if (tme == null) {
                    throw new BinlogException(ErrorCode.ERR_DUMP_NO_TABLE, new Exception("binlog file " + binlogFile + ", binlog pos " + event.getLogPos()
                            + ", event type " + eventType + ",获取table map log event 失败 "), "");
                }

                String table = tme.getTableName();
                String db = tme.getDbName();
                // 判断是否是 是pt online 工具生成的临时表 如果是临时表所有的数据都过滤掉
                if (ptOnlineTableFilter.filter(table)) {
                    return true;
                }

                carrier.car.db = db;
                carrier.car.table = table;
                carrier.car.tableMeta = updateTableMetaByCase(tme.getColumnInfo(), db, table, meta);
                if (carrier.car.tableMeta == null) {
                    throw new BinlogException(ErrorCode.ERR_DUMP_NO_TABLE, new Exception("获取表 " + db + "." + table + "元数据信息失败"), db + "." + table);
                }

                try {
                    TableRowsParser.parse(carrier); // 到这里需要将row进行解析 到具体的数值 开始并行发送
                } catch (Throwable exp) {
                    LogUtils.error.error("表 " + db + "." + table + " 解析失败 协议不兼容/被更改", exp);
                    throw new BinlogException(ErrorCode.WARN_MySQL_ROWEVENT_PARSE, new Exception(db + "." + table + " 解析失败 协议不兼容"),
                            "file=" + binlogFile + ", pos=" + event.getLogPos()
                                    + ", type=" + eventType);
                }
                break;
            case LogEvent.GTID_LOG_EVENT:

                GtidLogEvent gtidLogEvent = (GtidLogEvent) event;
                GTID newGtid = new GTID(gtidLogEvent.getGtidGNO(), gtidLogEvent.getSid());

                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("new gtid:" + newGtid.toString() + ",terminal gtid : " + metaInfo.getTerminal());
                }

                Meta.Terminal terminal = null;
                if ((terminal = metaInfo.getTerminal()) != null &&
                        compare(newGtid.toString(), terminal.getGtid()) >= 0) {
                    isTerminalGtid = true;
                }

                LogUtils.debug.debug("is terminal gtid ? " + isTerminalGtid);

                if (gtids.size() == 0) {
                    gtids.put(newGtid.sid, newGtid);
                } else {
                    addNewGtid(gtids, newGtid);
                }
                return true;
            case LogEvent.XID_EVENT:
                isCommit = true;

                // set transaction id
                trxID = ((XidLogEvent) event).getXid();
                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("transaction id " + trxID);
                }
                break;
            default:
                return true;
        }

        LogPosition logPos = new LogPosition(
                binlogFile,
                event.getLogPos(),
                event.getWhen(),
                isCommit, ruleNum);
        carrier.car.logPos = logPos;
        carrier.car.trxID = trxID + 1;

        if (gtids.size() != 0) { // MySQL 5.5是没有gtid 并且高版本的MySQL也可能不开gtid
            logPos.addAllGtids(gtids);
            logPos.refresh();
        }

        // 多线程发送版本不允许强制同步binlog 位置
        logPositions.offer(logPos);

        // 日志记录解析时间
        PerformanceUtils.perform(PerformanceUtils.PARSE_DELAY_KEY, event.getWhen());

        // 放入队列
        IBinlogHandler handler = this.handler;
        if (handler != null) {
            handler.offer(carrier);
        }

        return false;
    }

    /**
     * 根据ddl变更表结构信息
     *
     * @return isCommit
     */
    private final boolean updateTableMetaByDDL(QueryLogEvent qe, TableMetaCache tableMetaCache, Carrier carrier) {
        String query = qe.getQuery();
        String upper = CharUtils.toUpperCase(query);

        // 非commit or begin 前提下
        if (StringUtils.equals(upper, COMMIT)) {
            trxID += 1;
            if (LogUtils.debug.isDebugEnabled()) {
                LogUtils.debug.debug("commit sql transaction id " + trxID);
            }
        } else if (!StringUtils.endsWith(upper, BEGIN)) {

            carrier.car.db = qe.getDbName();
            for (String sql : query.split(";")) {
                SimpleDdlParser.DdlResult result = SimpleDdlParser.parse(sql.trim(), qe.getDbName());
                String schema = qe.getDbName();

                switch (result.getType()) {
                    case RENAME:
                        for (SimpleDdlParser.DdlResult rst : result.getRsts()) {
                            handleDDLParseResult(rst, schema, tableMetaCache, carrier);
                        }
                        break;
                    default:
                        handleDDLParseResult(result, schema, tableMetaCache, carrier);
                        break;
                }
            }
            return true; // ddl 语句也需要保存不能够直接从队列当中拿走 兼容gtid操作
        }

        return false;
    }

    /**
     * handle ddl result
     *
     * @param rst
     * @param schema
     */
    private final void handleDDLParseResult(SimpleDdlParser.DdlResult rst, String schema,
                                            TableMetaCache tableMetaCache, Carrier carrier) {
        if (StringUtils.isNotEmpty(rst.getSchemaName())) {
            schema = rst.getSchemaName();
        }
        String table = rst.getTableName();

        if (table == null || schema == null || rst.getType().equals(WaveEntry.EventType.ERASE)) {
            return;
        }
        // 如果新的表名是 _old 或者 _new形式 则直接放弃不需要table meta
        if (!ptOnlineTableFilter.filter(rst.getTableName())) {
            // 非begin commit 由于过滤规则需要 也需要将库表写入到carrier 载体当中
            carrier.car.table = rst.getTableName();

            tableMetaCache.refreshTableCache(schema, rst.getTableName());
        }
    }

    /**
     * 根据binlog事件当中列的数量 与 缓存当中列的数量是否一致
     * <p>
     * 决定是否需要刷新缓存的表结构信息
     *
     * @param columnInfo
     * @param db
     * @param table
     * @param tableMetaCache
     * @return TableMeta
     */
    private TableMeta updateTableMetaByCase(TableMapLogEvent.ColumnInfo[] columnInfo,
                                            String db, String table,
                                            TableMetaCache tableMetaCache) {
        if (db == null || table == null) {
            return null; // example like: CREATE DATABASE IF NOT EXITS BDAUTH
        }
        TableMeta tableMeta = tableMetaCache.getTableMeta(db, table, true);

        if (tableMeta == null) {
            tableMeta = tableMetaCache.refreshTableCache(db, table); // 如果表结构不存在 直接强制刷新
        }

        // check table fileds count，只能处理加字段
        if (tableMeta != null && columnInfo.length > tableMeta.getColumnSize()) {
            // online ddl增加字段操作步骤：
            // 1. 新增一张临时表，将需要做ddl表的数据全量导入
            // 2. 在老表上建立I/U/D的trigger，增量的将数据插入到临时表
            // 3. 锁住应用请求，将临时表rename为老表的名字，完成增加字段的操作
            // 尝试做一次reload，可能因为ddl没有正确解析，或者使用了类似online ddl的操作
            // 因为online ddl没有对应表名的alter语法，所以不会有clear cache的操作
            tableMeta = tableMetaCache.refreshTableCache(db, table);

            // 再做一次判断
            if (columnInfo.length > tableMeta.getColumnSize()) {
                LogUtils.warn.warn("column size is not match for table:"
                        + tableMeta.getFullName() + ","
                        + columnInfo.length + " vs " + tableMeta.getColumnSize());
            }
        }
        return tableMeta;
    }

    /**
     * add new gtid into gtids
     *
     * @param gtids
     * @param newGtid
     */
    private final void addNewGtid(Map<String, GTID> gtids, GTID newGtid) {
        String sid = newGtid.sid;
        if (gtids.containsKey(sid)) {
            GTID gtid = gtids.get(sid);
            newGtid.mergeGtid(gtid);
            gtids.put(sid, newGtid);
            return;
        }

        gtids.put(sid, newGtid);
    }

    private void prepare() throws BinlogException {
        LogUtils.debug.debug("prepare");
        this.originPos = getLogPos();
        this.dump.connect();
        updateConnectionProps(dump);

        if (originPos.hasGTID() && metaInfo.getBinlogInfo().getWithGTID()) {
            LogUtils.info.info(host + " binlog dump : GTID DUMP");

            BinlogDump.sendDumpCommand(dump, DumpType.COM_BINLOG_DUMP_GTID, metaInfo);

        } else {
            LogUtils.info.info(host + " binlog dump : COMMON DUMP");

            BinlogDump.sendDumpCommand(dump, DumpType.COM_BINLOG_DUMP, metaInfo);
        }

        // 过滤事件 而不是dump 全部事件
        decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);

        context = new LogContext();
        context.setLogPosition(
                new LogPosition(metaInfo.getBinlogFileName(), metaInfo.getBinlogPosition(),
                        metaInfo.getBinlogInfo().getBinlogWhen(), true));
    }

    /**
     * update MySQL connection properties
     *
     * @param connector
     * @throws IOException
     */
    protected void updateConnectionProps(MySQLConnector connector) {
        MySQLExecutor exe = new MySQLExecutor(connector);
        exe.setConnCfg(metaInfo.getDbInfo().getSlaveUUID());
    }

    /**
     * gtid 格式： 9583d493-ce62-11e6-91a1-507b9d578e91:1-4:6-9:12-15:18
     *
     * @return LoPostion get origin log position
     */
    private LogPosition getLogPos() {
        Meta.BinlogInfo binlogInfo = metaInfo.getBinlogInfo();
        LogPosition logPosition = new LogPosition(binlogInfo.getBinlogFile(), binlogInfo.getBinlogPos(),
                binlogInfo.getBinlogWhen(), true);

        if (binlogInfo.getExecutedGtidSets().length() > 0) {
            for (String gtidSet : binlogInfo.getExecutedGtidSets().split(",")) {
                logPosition.addGtid(GTID.parseGTID(gtidSet.split(":")));
            }
            logPosition.refresh();
        }
        LogUtils.info.info("origin pos : " + logPosition.getGtidSets());
        return logPosition;
    }
}
