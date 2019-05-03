package com.jd.binlog.producer;

import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.meta.Meta;

import java.lang.reflect.Constructor;
import java.util.List;

/**
 * Created on 18-5-23
 *
 * @author pengan
 */
public class Producer {
    /**
     * 根据类名 反射生成 producer 避免每次生成都需要修改代码 只需要自己实现就 ok
     *
     * @param name
     * @param paras
     * @return
     */
    public static IProducer initProducer(String name, List<Meta.Pair> paras, String topic) {
        Class cl = null;
        try {
            cl = Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_CLASS_NOT_FOUND, e);
        }

        Constructor cons = null;
        try {
            cons = cl.getConstructor(List.class, String.class);
        } catch (NoSuchMethodException e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_CONSTRUCTOR,
                    "should implement constructor method like JMQProducer(List<Meta.Pair> paras)");
        }

        try {
            return (IProducer) cons.newInstance(paras, topic);
        } catch (Exception e) {
            throw new BinlogException(ErrorCode.ERR_PRODUCER_ERR_INIT, e);
        }
    }
}
