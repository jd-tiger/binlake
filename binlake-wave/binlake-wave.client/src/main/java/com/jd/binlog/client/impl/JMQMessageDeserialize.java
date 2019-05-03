package com.jd.binlog.client.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.binlog.client.EntryMessage;
import com.jd.binlog.client.MessageDeserialize;
import com.jd.binlog.client.WaveEntry;
import com.jd.jmq.common.message.Message;
import org.apache.log4j.Logger;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by pengan on 17-3-8.
 * <p>
 * <p>
 * <p>
 * 将MQ消息队列当中的消息解析成EntryMessage格式
 */
public class JMQMessageDeserialize implements MessageDeserialize<Message> {
    private static final Logger logger = Logger.getLogger(JMQMessageDeserialize.class);

    /**
     * convert jmq message to WaveEntry List
     *
     * @param msgs messages from jmq
     * @return not null
     */
    public List<EntryMessage> deserialize(List<Message> msgs) throws InvalidProtocolBufferException {
        logger.debug("convert JMQ message to EntryMessage");
        List<EntryMessage> entries = new LinkedList<EntryMessage>();
        /**
         * if msgs size == 0 then no message to return
         */
        if (msgs.size() == 0) {
            logger.debug("mq message size = 0");
            return entries;
        }

        for (Message msg : msgs) {
            byte[] msgBody = null;
            if ((msgBody = msg.getByteBody()) != null) {
                // parse body which store the WaveEntry byteArray
                WaveEntry.Entry entry = WaveEntry.Entry.parseFrom(msgBody);

                // create new EntryMessage to store data
                EntryMessage entryMessage = new EntryMessage(msg);
                entryMessage.setHeader(entry.getHeader());
                entryMessage.setBatchId(entry.getBatchId());
                entryMessage.setInId(entry.getInId());
                entryMessage.setIp(entry.getIp());

                // get entry type then classify entry type
                switch (entry.getEntryType()) {
                    case ROWDATA:
                        WaveEntry.RowChange rowChange = WaveEntry.RowChange.parseFrom(entry.getStoreValue());
                        entryMessage.setEntryType(WaveEntry.EntryType.ROWDATA);
                        entryMessage.setRowChange(rowChange);
                        break;
                    case HEARTBEAT:
                        break;
                    case TRANSACTIONEND:
                        WaveEntry.TransactionEnd end = WaveEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                        entryMessage.setEntryType(WaveEntry.EntryType.TRANSACTIONEND);
                        entryMessage.setEnd(end);
                        break;
                    case TRANSACTIONBEGIN:
                        WaveEntry.TransactionBegin begin = WaveEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                        entryMessage.setEntryType(WaveEntry.EntryType.TRANSACTIONBEGIN);
                        entryMessage.setBegin(begin);
                        break;
                }

                entries.add(entryMessage);
            }
        }
        logger.debug("entry message size = " + entries.size());
        return entries;
    }
}
