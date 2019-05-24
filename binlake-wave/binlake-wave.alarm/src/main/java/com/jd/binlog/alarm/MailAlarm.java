package com.jd.binlog.alarm;

import java.util.Map;

/**
 * @author pengan 2019年05月23日
 */
public class MailAlarm extends Alarmer {

    private String[] adminMails;

    /**
     * @param paras: url, token
     * @param ms:    admin mails
     */
    public MailAlarm(Map<String, String> paras, String[] ms) {
        this.url = paras.getOrDefault(KEYWORD_URL, "http://api.dbs.jd.com:9000/godbs/sendText/");
        this.token = paras.getOrDefault(KEYWORD_TOKEN, "J1itu2OlRF2ThgWYK8yPcQ==");
        this.adminMails = ms;
    }

    public String[] getAdminMails() {
        return adminMails;
    }
}
