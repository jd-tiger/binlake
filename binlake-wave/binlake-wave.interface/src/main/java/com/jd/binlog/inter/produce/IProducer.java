package com.jd.binlog.inter.produce;

import com.jd.binlog.inter.rule.IRule;

/**
 * Created on 18-5-15
 *
 * @attention 注意需要实现构造方法 @eg {JMQProducer(List<Meta.Pair> paras, String topic)}
 *
 * @author pengan
 */
public interface IProducer<T> {
    /**
     * produce message 生产消息 异常需要增加充实次数
     */
    void produce(IRule.RetValue<T> msg) throws Exception;

    /**
     *
     * close 释放链接
     */
    void close();
}
