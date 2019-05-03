package com.jd.binlog.util;

/**
 * Created by pengan on 16-12-21.
 * time util
 */
public class TimeUtils {

    private static final int LEFT_MOVE_BITS = 16;

    public static long time() {
        return System.currentTimeMillis();
    }

    /**
     * using milis to calculate then convert to minutes
     *
     * approximately using 2^16 as 60000
     * @return minutes
     */
    public static long calculateTimeSpan(long lastTime) {
        return ((time() - lastTime) >> LEFT_MOVE_BITS);
    }
}
