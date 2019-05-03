package com.jd.binlog.alarm;

/**
 * Created on 18-7-20
 * <p>
 * 普通报警
 *
 * @author pengan
 */
public class AlarmUtils {
    private static Alarmer alarmer = new Alarmer();

    public static void alarm(String msg) {
        alarmer.alarm(msg);
    }
}
