package com.jd.binlog.convert;

import com.jd.binlog.inter.filter.IFilter;
import com.jd.binlog.inter.msg.IConvert;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.msg.IRepartition;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IKeyGenerator;
import com.jd.binlog.inter.work.IBinlogWorker;

import java.util.BitSet;

/**
 * Created on 18-7-16
 *
 * @author pengan
 */
public class AvroConverter implements IConvert<String> {
    @Override
    public void appendFakeColumns(IFilter filter, BitSet bs, IMessage msg) {

    }

    @Override
    public void formatUpdateByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatInsertByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatDeleteByOneRow(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatBeginQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatCommitQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatDDLQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatCommitXID(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatRowsQuery(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatUserVar(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatIntVar(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatRand(IMessage msg, String id, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatInsertByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatUpdateByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }

    @Override
    public void formatDeleteByRows(IMessage msg, String topic, IKeyGenerator generator, IRepartition part, IProducer producer, IBinlogWorker worker) {

    }
}
