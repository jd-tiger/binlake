package com.jd.binlog.alarm;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created on 18-7-20
 * <p>
 * 普通报警
 *
 * @author pengan
 */
public class AlarmUtils {
    public static Alarmer mail;
    public static Alarmer phone;

    public static void init(Map<String, String> m, String[] ms, Map<String, String> p, String[] ps) {
        AlarmUtils.mail = new MailAlarm(m, ms);
        AlarmUtils.phone = new PhoneAlarm(p, ps);
    }

    public static String[] adminMails() {
        return ((MailAlarm)mail).getAdminMails();
    }

    public static String[] adminPhones() {
        return ((PhoneAlarm)phone).getAdminPhones();
    }

    /**
     * mail using JDMailParas
     *
     * @param data: JDMailParas generate map
     */
    public static void mail(int retry, int latch, Map<String, Object> data) {
        if (retry < latch - 2) {
            return;
        }
        // 低于 latch 2 次开始报警
        data.put("retry times > retry latch", String.format("%d > %d", retry, latch));
        try {
            mail.alarm(data);
        } catch (UnsupportedEncodingException e) {
        }
    }

    /***
     * phone using JDPhoneParas 系统报警
     *
     * @param data: JDPhoneParas generate map
     */
    public static void phone(Map<String, Object> data) {
        try {
            phone.alarm(data);
        } catch (UnsupportedEncodingException e) {
        }
    }
}
