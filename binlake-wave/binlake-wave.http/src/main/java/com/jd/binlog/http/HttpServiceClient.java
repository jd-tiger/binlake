package com.jd.binlog.http;

import com.jd.binlog.meta.Http;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.util.RunCallUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import sun.misc.IOUtils;

import java.util.concurrent.Callable;

/**
 * http service client
 *
 * @author pengan
 */
public class HttpServiceClient {

    private String urlPre;

    public HttpServiceClient(String host, int port) {
        this.urlPre = "http://" + host + ":" + port;
    }

    /**
     * take local agent candidates
     *
     * @return
     * @throws Exception
     */
    public String[] candidates() throws Exception {
        LogUtils.debug.debug("kill on client ");

        StringEntity entity = new StringEntity("");
        String url = this.urlPre + "/candidates";
        return execute(entity, url, bytes -> new String(bytes).split(","), () -> new String[]{});
    }

    /**
     * add leader selector
     *
     * @param req Http.RefreshRequest
     * @return
     * @throws Exception
     */
    public Http.Response addLeaderSelector(Http.RefreshRequest req) throws Exception {
        StringEntity entity = new StringEntity(new String(Http.RefreshRequest.marshal(req)));
        String url = this.urlPre + "/add/selector";
        return execute(entity, url, bytes -> Http.Response.unmarshalJson(bytes), () -> Http.Response.FAILURE);
    }

    /**
     * @param version: meta version
     * @param key:     zookeeper db key
     * @return
     * @throws Exception
     */
    public Http.Response kill(long version, String key) throws Exception {
        LogUtils.debug.debug("kill on client ");

        byte[] req = Http.KillRequest.marshal(new Http.KillRequest(key, version));
        ByteArrayEntity entity = new ByteArrayEntity(req);
        String url = this.urlPre + "/kill";
        return execute(entity, url, bytes -> Http.Response.unmarshalJson(bytes), () -> Http.Response.FAILURE);
    }


    /**
     * @return to judge whether the sever is alive
     * @throws Exception
     */
    public Http.Response isAlive() throws Exception {
        StringEntity entity = new StringEntity("");
        String url = this.urlPre + "/alive";
        return execute(entity, url, bytes -> Http.Response.unmarshalJson(bytes), () -> Http.Response.FAILURE);
    }


    /**
     * execute http entity
     *
     * @param entity: http entity
     * @param url
     * @return
     * @throws Exception
     */
    private <V> V execute(HttpEntity entity, String url, RunCallUtils<byte[], V> call, Callable<V> fail) throws Exception {
        CloseableHttpClient client = HttpClients.createDefault();
        try {
            HttpPost httpPost = new HttpPost(url);

            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");

            CloseableHttpResponse resp = client.execute(httpPost);

            if (resp == null) {
                LogUtils.warn.warn("no response from server " + url);
                return fail.call();
            }

            int c = resp.getStatusLine().getStatusCode();
            if (c != 200) {
                LogUtils.warn.warn("response status code from server " + c);
                return fail.call();
            }

            return call.call(IOUtils.readFully(resp.getEntity().getContent(), -1, false));
        } finally {
            client.close();
        }
    }
}
