package com.jd.binlog.inter.perform;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public interface IPerformance {

    /**
     * 开启性能检查 标志
     */
    AtomicBoolean PERFORM_ACCESS = new AtomicBoolean(false);

    /**
     * key 值表示阶段
     * when binlog 产生
     *
     * @param key
     * @param when
     */
    void perform(String key, long when);


    /**
     * 当前时间
     *
     * @para key
     * @param when
     * @param curr
     */
    void perform(String key, long when, long curr);
}
