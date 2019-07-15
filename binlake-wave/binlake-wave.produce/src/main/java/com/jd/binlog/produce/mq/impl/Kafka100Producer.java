package com.jd.binlog.produce.mq.impl;

import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.produce.mq.MQProducer;
import com.jd.binlog.util.LogUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 18-6-28
 *
 * @author pengan
 */
public class Kafka100Producer extends MQProducer<ProducerRecord> {
    AtomicBoolean isClosed = new AtomicBoolean(false);
    private KafkaProducer<byte[], byte[]> producer;
    int partition;

    public Kafka100Producer(List<Meta.Pair> paras, String topic) {
        Properties props = new Properties();
        for (Meta.Pair pair : paras) {
            props.put(pair.getKey(), pair.getValue());
        }
        producer = new KafkaProducer<>(props);

        // 由于需要严格保证顺序 所以强制指定partition
        partition = producer.partitionsFor(topic).size();
    }

    @Override
    public void produce(IRule.RetValue<String> msg) throws Exception {
        KafkaProducer<byte[], byte[]> producer = this.producer;
        if (isClosed.get() || producer == null) {
            return; // do nothing
        }

        // have to wait all message send to server
        int part = msg.partition % partition;

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("kafka partition is " + part + ", total partition is " + partition
                    + ", message key " + new String(msg.key));
        }

        // 1 seconds for producer
        Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(msg.id, part, msg.key, msg.value));

        future.get(2, TimeUnit.SECONDS); // todo 测试 阻塞长时间等待
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        KafkaProducer producer = this.producer;
        if (producer != null) {
            producer.close();
        }
        this.producer = null;
    }

    public byte[] formatKey(String key) {
        String tid = Thread.currentThread().getName().substring("producer ".length());
        StringBuilder kb = new StringBuilder();
        if (key.length() > 7) {
            kb.append(key.substring(key.length() - 7));
        } else {
            kb.append(key);
        }

        kb.append("_");
        String ts = System.nanoTime() / 100000 + "";
        kb.append(ts.substring(ts.length() - 5)).append("_").append(tid);
        return kb.toString().getBytes();
    }

    public static void main(String[] args) throws Exception {

        String topic = "yinipromotiontest";
        List<Meta.Pair> paras = new LinkedList<>();

        paras.add(newPair("systemId", "binlake"));
        paras.add(newPair("token", "mq"));

        paras.add(newPair("destMark", "binlake"));

        paras.add(newPair("timeout", "120"));
        paras.add(newPair("bootstrap.servers", "192.168.212.62:50088"));

        paras.add(newPair("acks", "1"));
        paras.add(newPair("request.timeout.ms", "1000"));

        paras.add(newPair("batch.size", "0"));
        paras.add(newPair("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));
        paras.add(newPair("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));

        IProducer<String> p = initProducer("com.jd.binlog.produce.mq.impl.Kafka100Producer", paras, topic);

        IRule.RetValue<String> v = new IRule.RetValue<>();
        v.partition = 100;
        v.key = "pengan3".getBytes();
        v.id = topic;
        v.value = "pengan3".getBytes();

        p.produce(v);

    }

    public static IProducer initProducer(String name, List<Meta.Pair> paras, String topic) {
        Class cl = null;
        try {
            cl = Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_CLASS_NOT_FOUND, e, name);
        }

        Constructor cons = null;
        try {
            cons = cl.getConstructor(List.class, String.class);
        } catch (NoSuchMethodException e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_CONSTRUCTOR,
                    new Exception("should implement constructor method like JMQProducer(List<Meta.Pair> paras, String topic)"), "");
        }

        try {
            return (IProducer) cons.newInstance(paras, topic);
        } catch (Exception e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_ERR_INIT, e, paras + "topic=" + topic);
        }
    }


    private static Meta.Pair newPair(String key, String value) {
        Meta.Pair p = new Meta.Pair();
        p.setKey(key);
        p.setValue(value);
        return p;
    }
}
