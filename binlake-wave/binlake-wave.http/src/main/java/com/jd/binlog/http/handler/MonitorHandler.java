package com.jd.binlog.http.handler;

import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.util.JsonUtils;
import com.jd.binlog.util.LogUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * monitor handler get monitor data which is sended to agent
 *
 * @author pengan
 */
public class MonitorHandler implements HttpHandler {
    private ConcurrentHashMap<String, ILeaderSelector> lsm;

    public MonitorHandler(ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) {
        this.lsm = lsm;
    }

    @Override
    public void handle(HttpExchange exc) throws IOException {
        LogUtils.debug.debug("delay handle");

        // delay map
        Map<String, Object> d = new HashMap<>();

        String key; // ILeaderSelector key
        MetaInfo m;   // meta info
        Map<String, Object> mo; // monitor
        ILeaderSelector l; // leader selector
        IBinlogWorker w; // worker
        for (Map.Entry<String, ILeaderSelector> entry : lsm.entrySet()) {
            Map<String, Object> node = new HashMap<>();
            key = entry.getKey();
            l = entry.getValue();

            // initiate for each time
            if (l != null && (w = l.getWork()) != null) {
                if ((mo = w.monitor()) != null) {
                    // take all monitor inside
                    node.putAll(mo);
                }

                if ((m = l.getMetaInfo()) != null) {
                    // leader version & meta version & candidates
                    node.put("leaderVersion", m.getLeaderVersion());
                    node.put("metaVersion", m.getDbInfo().getMetaVersion());
                    node.put("candies", m.getCandidate());
                }
            }
            d.put(key, node);
        }

        exc.sendResponseHeaders(200, 0);
        OutputStream os = exc.getResponseBody();
        os.write(JsonUtils.ObjtoJson(d).getBytes());
        os.close();
    }
}
