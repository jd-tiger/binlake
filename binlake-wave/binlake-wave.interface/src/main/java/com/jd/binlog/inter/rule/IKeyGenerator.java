package com.jd.binlog.inter.rule;

import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.util.HashUtils;
import com.jd.binlog.util.TimeUtils;

import java.util.BitSet;

/**
 * Created on 18-5-16
 * <p>
 * 键值 生成器
 *
 * @author pengan
 */
public interface IKeyGenerator {
    /**
     * 业务主键消息顺序 --> 表消息顺序 --> 实例消息顺序
     */

    // 限制 队列的个数  1023个 取与 防止出现负数
    int MAX_QUEUE_SIZE = (1 << 10) - 1;

    /**
     * 生成 键值
     *
     * @param rv
     * @param msg
     */
    void generate(IRule.RetValue rv, IMessage msg);

    class RandOrder implements IKeyGenerator {

        /**
         * 随机的 键值
         * <p>
         * 高低字节转换
         *
         * @return
         */
        @Override
        public void generate(IRule.RetValue rv, IMessage msg) {
            rv.key = (TimeUtils.time() + "").getBytes(); // 时间戳作为 key值
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }

    class BusinessOrder extends TableOrder {

        /**
         * 根据业务主键 生成键值
         *
         * @param msg
         * @param rv
         * @return
         */
        @Override
        public void generate(IRule.RetValue rv, IMessage msg) {
            BitSet bitSet = msg.getKeyBitSet();
            if (bitSet == null) {
                super.generate(rv, msg); // 业务主键 消息顺序 无业务主键 则采用
                return;
            }

            String[] rowVals = msg.currRowVals(); // 获取当前行位置 业务主键
            StringBuilder key = new StringBuilder();
            for (int i = 0; i < rowVals.length; i++) {
                if (bitSet.get(i)) {
                    key.append(rowVals[i]);
                }
            }

            rv.key = key.toString().getBytes();
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }


    class TableOrder extends DbOrder {

        /**
         * 根据表名 生成键值
         *
         * @param msg
         * @param rv
         */
        public void generate(IRule.RetValue rv, IMessage msg) {
            if (msg.getTable() == null) {
                super.generate(rv, msg); // 表消息顺序 如果表名是空 则按照实例顺序发送
                return;
            }
            rv.key = msg.getTable().getBytes();
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }

    class DbOrder extends InstanceOrder {

        /**
         * 根据库名 生成键值
         *
         * @param msg
         * @param rv
         */
        public void generate(IRule.RetValue rv, IMessage msg) {
            if (msg.getDb() == null) {
                super.generate(rv, msg); // 表消息顺序 如果表名是空 则按照实例顺序发送
                return;
            }
            rv.key = msg.getDb().getBytes();
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }


    class InstanceOrder implements IKeyGenerator {

        /**
         * 根据 host 生成键值
         *
         * @param rv
         * @param msg
         */
        @Override
        public void generate(IRule.RetValue rv, IMessage msg) {
            rv.key = msg.getHostBytes();
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }

    class TransactionOrder implements IKeyGenerator {
        /**
         * 根据 transaction id 来生成 键值
         *
         * @param rv
         * @param msg
         */
        @Override
        public void generate(IRule.RetValue rv, IMessage msg) {
            rv.key = (msg.getTrxID() + "").getBytes();
            rv.partition = (int) (HashUtils.hash(rv.key) & MAX_QUEUE_SIZE);
        }
    }
}
