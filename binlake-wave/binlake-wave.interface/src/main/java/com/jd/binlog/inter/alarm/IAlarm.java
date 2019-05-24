package com.jd.binlog.inter.alarm;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public interface IAlarm {
    /**
     * 邮件主题模板
     */
    String MailSubTemplate = "BinLake wave %s dump MySQL实例%s 异常";

    /***
     * url
     */
    String KEYWORD_URL = "url";

    /***
     * token key word
     */
    String KEYWORD_TOKEN = "token";

    void alarm(Map<String, Object> data) throws UnsupportedEncodingException;
}
