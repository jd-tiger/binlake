package com.jd.binlog.util;

/**
 * Run call
 *
 * @param <T>
 * @param <V>
 */
public interface RunCallUtils<T, V> {
    V call(T t) throws Exception;
}
