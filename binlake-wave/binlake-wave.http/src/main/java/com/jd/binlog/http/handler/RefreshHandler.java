package com.jd.binlog.http.handler;

import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Http;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import sun.misc.IOUtils;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * refresh candidate, new leader and so on to lower the wave delay
 *
 * @author pengan
 */
public class RefreshHandler implements HttpHandler {
    private ConcurrentHashMap<String, ILeaderSelector> lsm;

    public RefreshHandler(ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) {
        this.lsm = lsm;
    }

    @Override
    public void handle(HttpExchange exc) throws IOException {
        try {
            Http.RefreshRequest req = Http.RefreshRequest.unmarshalJson(IOUtils.readFully(exc.getRequestBody(), -1, false));

            ILeaderSelector l = this.lsm.get(req.getKey());
            if (l != null) {
                l.refreshCandidate(req);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
