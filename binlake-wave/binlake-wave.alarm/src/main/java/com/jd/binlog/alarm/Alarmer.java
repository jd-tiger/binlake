package com.jd.binlog.alarm;

import com.jd.binlog.inter.alarm.IAlarm;
import com.jd.binlog.util.HttpUtils;
import com.jd.binlog.util.JsonUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public class Alarmer implements IAlarm {

    protected String url;
    protected String token;

    @Override
    public void alarm(Map<String, Object> data) throws UnsupportedEncodingException {
        HttpPost post = new HttpPost(url);
        StringEntity params = new StringEntity(JsonUtils.ObjtoJson(data));
        post.addHeader("content-type", "application/json");
        post.setHeader("token", token);
        post.setEntity(params);

        try {
            HttpUtils.jsonRequest(post);
        } catch (Exception e) {
            LogUtils.error.error("alarm " + url + " error", e);
        }
    }
}
