package com.jd.binlog.http.handler;

import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Http;
import com.jd.binlog.util.LogUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * add leader selector using zk client
 *
 * @author pengan
 */
public class AddSelectorHandler implements HttpHandler {
    private ConcurrentHashMap<String, ILeaderSelector> lsm;
    private IZkClient c;


    public AddSelectorHandler(ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) {
        this.lsm = lsm;
        this.c = client;
    }

    @Override
    public void handle(HttpExchange exc) throws IOException {
        // add leader selector
        try {
            Http.RefreshRequest req = Http.RefreshRequest.unmarshalJson(IOUtils.readFully(exc.getRequestBody(), -1, false));
            LogUtils.debug.debug(req);
            ILeaderSelector l;

            if ((l = lsm.get(req.getKey())) == null) {
                // leader not exist
                c.addLeaderSelector(req);
            } else {
                // leader already inside then just pass
            }
        } catch (Exception e) {
            LogUtils.error.error(e);
        }

        exc.sendResponseHeaders(200, 0);
        OutputStream os = exc.getResponseBody();
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
