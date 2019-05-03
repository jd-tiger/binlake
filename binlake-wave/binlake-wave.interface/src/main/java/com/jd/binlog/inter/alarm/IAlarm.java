package com.jd.binlog.inter.alarm;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public interface IAlarm {
    AtomicLong retryTimes = new AtomicLong();
    Set<String> phones = new LinkedHashSet<>();
    AtomicReference<String> token = new AtomicReference<>();
    AtomicReference<String> url = new AtomicReference<>();

    void alarm(String msg);
}
