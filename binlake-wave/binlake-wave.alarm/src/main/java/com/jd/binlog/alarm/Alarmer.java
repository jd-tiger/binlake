package com.jd.binlog.alarm;

import com.jd.binlog.inter.alarm.IAlarm;
import com.jd.binlog.util.HttpUtils;
import com.jd.binlog.util.LogUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public class Alarmer implements IAlarm {

    @Override
    public void alarm(String msg) {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("text", msg);

        data.put("mobile_nums", phones.toArray());
        try {
            HttpUtils.jsonRequest(url.get(), token.get(), data);
        } catch (Exception e) {
            LogUtils.error.error("alarm " + url + " error", e);
        }
    }
}
