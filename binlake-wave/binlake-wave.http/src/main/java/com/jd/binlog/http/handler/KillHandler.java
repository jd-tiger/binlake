package com.jd.binlog.http.handler;

import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Http;
import com.jd.binlog.meta.MetaInfo;
import com.jd.binlog.util.JsonUtils;
import com.jd.binlog.util.LogUtils;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import sun.misc.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kill leader
 *
 * @author pengan
 */
public class KillHandler implements HttpHandler {
    private ConcurrentHashMap<String, ILeaderSelector> lsm;

    public KillHandler(ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) {
        this.lsm = lsm;
    }

    @Override
    public void handle(HttpExchange exc) throws IOException {
        LogUtils.debug.debug("kill");

        Http.Response state = Http.Response.SUCCESS;

        try {
            Http.KillRequest req = Http.KillRequest.unmarshalJson(IOUtils.readFully(exc.getRequestBody(), -1, false));

            ILeaderSelector ls;
            MetaInfo cm; //

            if ((ls = lsm.get(req.getKey())) != null) {
                /**
                 * if present meta is younger than the para meta
                 *
                 * session is to close, para meta should be the leader
                 */
                cm = ls.getMetaInfo();
                if (LogUtils.debug.isDebugEnabled()) {
                    LogUtils.debug.debug("present meta info : " + cm);
                }

                if (cm == null) {
                    // maybe already abandon leader and clear metaInfo = null
                    state = Http.Response.SUCCESS; //
                } else {
                    if (cm.getLeaderVersion() < req.getLeaderVersion()) {
                        if (LogUtils.debug.isDebugEnabled()) {
                            LogUtils.debug.debug("{meta:" + req.getKey() + "}, is closed");
                        }

                        // 停止dump 放弃leader 并不退出leader selector
                        ls.killWorkAndAbandonLeader();
                    } else {
                        state = Http.Response.FAILURE;
                    }
                }
            }
        } catch (Exception e) {
            LogUtils.error.error("rpc kill error", e);
        }

        exc.sendResponseHeaders(200, 0);
        OutputStream os = exc.getResponseBody();
        os.write(JsonUtils.ObjtoJson(state).getBytes());
        os.close();
    }
}
