package com.jd.binlog.client.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.binlog.client.WaveEntry;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by ninet on 17-5-9.
 */
public class KafkaMessageDeserializer implements Deserializer<KafkaEntryMessage> {

    public void configure(Map<String, ?> map, boolean b) {

    }

    public KafkaEntryMessage deserialize(String s, byte[] bytes) {
        try {
            WaveEntry.Entry entry = WaveEntry.Entry.parseFrom(bytes);
            KafkaEntryMessage kafkaEntryMessage = new KafkaEntryMessage(entry);
            switch (entry.getEntryType()) {
                case TRANSACTIONBEGIN:
                    WaveEntry.TransactionBegin begin = WaveEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    kafkaEntryMessage.setEntryType(WaveEntry.EntryType.TRANSACTIONBEGIN);
                    kafkaEntryMessage.setBegin(begin);
                    break;
                case TRANSACTIONEND:
                    WaveEntry.TransactionEnd end = WaveEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    kafkaEntryMessage.setEntryType(WaveEntry.EntryType.TRANSACTIONEND);
                    kafkaEntryMessage.setEnd(end);
                    break;
                case HEARTBEAT:
                    break;
                case ROWDATA:
                    WaveEntry.RowChange rowChange = WaveEntry.RowChange.parseFrom(entry.getStoreValue());
                    kafkaEntryMessage.setEntryType(WaveEntry.EntryType.ROWDATA);
                    kafkaEntryMessage.setRowChange(rowChange);
                    break;
            }

            return kafkaEntryMessage;
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void close() {

    }
}
