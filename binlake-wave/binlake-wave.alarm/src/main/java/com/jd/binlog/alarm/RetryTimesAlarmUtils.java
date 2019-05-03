package com.jd.binlog.alarm;

import com.jd.binlog.inter.alarm.IAlarm;

/**
 * Created on 18-7-20
 *
 * 重试次数报警
 *
 * @author pengan
 */
public class RetryTimesAlarmUtils {

    public static void alarm(long times, String msg) {
        if (times < IAlarm.retryTimes.get()) {
            return;
        }
        AlarmUtils.alarm(msg);
    }
}
