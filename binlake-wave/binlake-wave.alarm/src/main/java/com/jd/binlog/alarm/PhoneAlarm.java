package com.jd.binlog.alarm;

import java.util.Map;

/**
 * PhoneAlarm
 *
 * @author pengan 2019年05月23日
 */
public class PhoneAlarm extends Alarmer {

    private String[] adminPhones;

    /**
     * @param paras
     * @param mp:   admin phones
     */
    public PhoneAlarm(Map<String, String> paras, String[] mp) {
        this.url = paras.getOrDefault(KEYWORD_URL, "http://api.dbs.jd.com:9000/godbs/sendText/");
        this.token = paras.getOrDefault(KEYWORD_TOKEN, "J1itu2OlRF2ThgWYK8yPcQ==");
        this.adminPhones = mp;
    }

    public String[] getAdminPhones() {
        return adminPhones;
    }
}
