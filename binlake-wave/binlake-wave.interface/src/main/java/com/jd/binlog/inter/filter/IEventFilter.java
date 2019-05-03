package com.jd.binlog.inter.filter;


import com.jd.binlog.exception.BinlogException;

/**
 * 数据过滤机制
 * 
 * @author jianghang 2012-7-20 下午03:51:27
 */
public interface IEventFilter<T> {

    boolean filter(T event) throws BinlogException;
}
