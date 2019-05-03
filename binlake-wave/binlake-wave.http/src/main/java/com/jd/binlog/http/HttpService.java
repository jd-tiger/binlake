package com.jd.binlog.http;

import com.jd.binlog.http.handler.*;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.util.LogUtils;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * http service 
 *
 * @author pengan
 */
public class HttpService {
    /**
     * http service for outer
     *
     * @param port
     * @param lsm
     * @param client
     * @throws IOException
     */
    public HttpService(int port, ConcurrentHashMap<String, ILeaderSelector> lsm, IZkClient client) throws IOException {
        LogUtils.debug.debug("http service start");
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/alive", new AliveHandler(lsm, client));
        server.createContext("/delay", new MonitorHandler(lsm, client));
        server.createContext("/kill", new KillHandler(lsm, client));
        server.createContext("/refresh", new RefreshHandler(lsm, client));
        server.createContext("/add/selector", new AddSelectorHandler(lsm, client));
        server.start();
    }
}
