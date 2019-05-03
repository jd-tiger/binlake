package com.jd.binlog.produce.mq.impl;

import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.produce.mq.MQProducer;
import com.jd.binlog.util.LogUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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
        producer = new KafkaProducer<byte[], byte[]>(props);

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
                new ProducerRecord<byte[], byte[]>(msg.id, part, msg.key, msg.value));

        try {
            future.get(2, TimeUnit.SECONDS); // todo 测试 阻塞长时间等待
        } catch (Throwable exp) {
            throw new Exception(exp);
        }
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
}
