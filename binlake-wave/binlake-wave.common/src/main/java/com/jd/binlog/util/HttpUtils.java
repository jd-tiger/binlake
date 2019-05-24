package com.jd.binlog.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created on 18-5-11
 *
 * @author pengan
 */
public class HttpUtils {
    private static final String CHARSET = "UTF-8";

    /***
     *              String url, String token, Map<String, Object> data
     *             StringEntity params = new StringEntity(JsonUtils.ObjtoJson(data));
     *             post.addHeader("content-type", "application/json");
     *             post.setHeader("token", token);
     *             post.setEntity(params);
     * @param post
     * @return
     * @throws Exception
     */
    public static JsonNode jsonRequest(HttpPost post) throws Exception {
        CloseableHttpClient client = null;
        CloseableHttpResponse rsp = null;
        try {
            client = HttpClientBuilder.create().build();
            rsp = client.execute(post);

            if (rsp.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new IOException("error response http status " + rsp.getStatusLine().getStatusCode());
            }

            HttpEntity entity = rsp.getEntity();
            if (entity != null) {
                String content = EntityUtils.toString(entity, CHARSET);
                return JsonUtils.parseJson(content);
            }
            return null;
        } catch (Throwable e) {
            LogUtils.error.error("request to url error", e);
            if (rsp != null) {
                try {
                    rsp.close();
                } catch (Throwable exp) {
                }
            }

            if (client != null) {
                try {
                    client.close();
                } catch (Throwable exp) {
                }
            }
            throw new Exception(e);
        }
    }

    public static void main(String[] args) throws UnsupportedEncodingException {
        String phones = "18515819096,15726816160";
        String token = "J1itu2OlRF2ThgWYK8yPcQ==";
        String url = "http://api.dbs.jd.com:9000/godbs/sendText/";
        Set<String> phs = new LinkedHashSet<>();
        for (String phone : phones.split(",")) {
            if (phone.trim().length() != 0) {
                phs.add(phone.trim());
            }
        }

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("text", "binlake -test ");

        data.put("mobile_nums", phs.toArray());

        HttpPost post = new HttpPost(url);
        StringEntity params = new StringEntity(JsonUtils.ObjtoJson(data));
        post.addHeader("content-type", "application/json");
        post.setHeader("token", token);
        post.setEntity(params);

        try {
            HttpUtils.jsonRequest(post);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
