package com.jd.binlog.inter.rule;


import com.jd.binlog.dbsync.LogPosition;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.work.IBinlogWorker;


/**
 * Created on 18-5-14
 * <p>
 * 参数 消息格式
 * 输出 key value : {byte[], byte[]}
 *
 * @author pengan
 */
public interface IRule {
    /**
     * convert message into
     *
     * @param msg
     * @return
     */
    void convert(IMessage msg);

    /**
     * 获取每个规则的优先级 决定处理顺序
     *
     * @return
     */
    long priority();

    /**
     * close rule
     */
    void close();

    class RetValue<T> {
        /**
         * binlog 位置
         */
        public LogPosition logPos;

        /**
         * binlog 工作线程
         */
        public IBinlogWorker worker;

        /**s
         * 生产者
         */
        public IProducer producer;

        /**
         * hash 完成 对应队列标志
         */
        public int partition;

        /**
         * 消息的 id 如果是MQ 则对应到 topic名称
         */
        public T id;

        /**
         * 所有消息都是按照key value形式
         */
        public byte[] key;

        /**
         * 对应消息体
         *
         */
        public byte[] value;

        /**
         * clear
         */
        public void clear() {
            key = null;
            id = null;
            value = null;
            worker = null;
            producer = null;
            logPos = null;
        }
    }
}
