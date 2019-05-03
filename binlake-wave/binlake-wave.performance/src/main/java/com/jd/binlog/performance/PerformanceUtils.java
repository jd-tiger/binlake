package com.jd.binlog.performance;

import com.jd.binlog.inter.perform.IPerformance;

/**
 * Created on 18-7-20
 * <p>
 * 性能检测工具 打印每个阶段日志耗时
 *
 * @author pengan
 */
public class PerformanceUtils {
    public static final String DUMP_DELAY_KEY = "ump.binlake.dump.delay";
    public static final String DECODE_DELAY_KEY = "ump.binlake.decode.delay";
    public static final String CONVERT_DELAY_KEY = "ump.binlake.convert.delay";
    public static final String PARSE_DELAY_KEY = "ump.binlake.parse.delay";
    public static final String SEND_DELAY_KEY = "ump.binlake.send.delay";
    public static final String PRODUCE_DELAY_KEY = "ump.binlake.produce.delay";

    private static final UMPPerformer performer = new UMPPerformer();

    /**
     * 传递参数
     * @param key
     * @param when
     */
    public static void perform(String key, long when) {
        if (!IPerformance.PERFORM_ACCESS.get()) { // 未开启 性能检测
            return;
        }
        performer.perform(key, when);
    }

    /**
     *
     * @param key
     * @param when
     * @param curr
     */
    public static void perform(String key, long when, long curr) {
        if (!IPerformance.PERFORM_ACCESS.get()) { // 未开启 性能检测
            return;
        }
        performer.perform(key, when, curr);
    }
}
