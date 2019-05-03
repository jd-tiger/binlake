package com.jd.binlog.http.handler;

import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Http;
import com.jd.binlog.util.LogUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

/**
 * check leader alive
 *
 * @author pengan
 */
public class AliveHandler implements HttpHandler {

    public AliveHandler(ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) {
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        OutputStream os = exchange.getResponseBody();
        byte[] data = null;
        try {
            data = Http.Response.marshal(Http.Response.SUCCESS);
        } catch (Exception e) {
            LogUtils.error.error(e);
        }
        os.write(data);
        os.close();
    }
}
