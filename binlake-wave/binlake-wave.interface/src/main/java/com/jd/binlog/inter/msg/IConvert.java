package com.jd.binlog.inter.msg;


import com.jd.binlog.inter.filter.IFilter;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IKeyGenerator;
import com.jd.binlog.inter.work.IBinlogWorker;

import java.util.BitSet;

/**
 * Created on 18-5-14
 *
 * @author pengan
 */
public interface IConvert<T> {
    String ISO_8859_1 = "ISO-8859-1";
    int version = 1;
    String BEGIN = "BEGIN";
    String COMMIT = "COMMIT";

    int MAX_PACKET_SIZE = 10 * 1024;

    /**
     * 拼装伪列信息
     *
     * @return
     */
    void appendFakeColumns(IFilter filter, BitSet bs, IMessage msg);

    /**
     * 格式化 消息 行事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatUpdateByOneRow(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 格式化 消息 行事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatInsertByOneRow(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 格式化 消息 行事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatDeleteByOneRow(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 格式化 query event 当中 begin 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatBeginQuery(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 格式化 query event 当中 commit 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatCommitQuery(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 格式化 query event 当中 ddl事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatDDLQuery(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 格式化 commit xid 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatCommitXID(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 格式化 row query 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatRowsQuery(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 格式化 user var 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatUserVar(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 格式化 int var事件
     *
     * @param msg
     * @param producer
     * @param worker
     * @return
     */
    void formatIntVar(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 格式化 rand 事件
     *
     * @param msg
     * @param id
     * @param producer
     * @param worker
     * @return
     */
    void formatRand(IMessage msg, T id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 可以多行组装成一个insert 消息体
     *
     * @param msg
     * @param topic
     * @param generator
     * @param part
     * @param producer
     * @param worker
     */
    void formatInsertByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);

    /**
     * 可以多行组装成一个update 消息体
     *
     * @param msg
     * @param topic
     * @param generator
     * @param part
     * @param producer
     * @param worker
     */
    void formatUpdateByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);


    /**
     * 可以多行组装成一个delete 消息体
     *
     * @param msg
     * @param topic
     * @param generator
     * @param part
     * @param producer
     * @param worker
     */
    void formatDeleteByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker);
}
