package com.jd.binlake.tower.server;

import com.jd.binlake.tower.api.*;
import com.jd.binlake.tower.config.ConfigLoader;
import com.jd.binlake.tower.config.TowerConfig;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

/**
 * Created by pengan on 17-3-2.
 * tower server for meta operation
 */
public class TowerServer {
    private static final Logger logger = Logger.getLogger(TowerServer.class);

    public static TowerConfig config;
    private static Server server;

    public void start() throws Exception {
        logger.info("start");

        ConfigLoader loader = new ConfigLoader();
        loader.load();
        config = loader.getConfig();

        server = new Server(config.getPort());
        bindHandler();
        server.start();
    }

    /**
     * bind handler for right url
     */
    private void bindHandler() {
        logger.debug("bindHandler");
        CreateZNodesHandler.register();
        RemoveNodeHandler.register();
        ExistNodeHandler.register();
        ResetCounterHandler.register();
        SetInstanceOffline.register();
        SetInstanceOnline.register();
        SetBinlogPosHandler.register();
        SetLeaderHandler.register();
        SetCandidateHandler.register();
        SetTerminalHandler.register();
        GetSlaveBinlogHandler.register();

        ContextHandlerCollection contexts = new ContextHandlerCollection();

        Handler[] handlers = new Handler[ApiCenter.CONTEXTS.size()];
        int index = 0;

        for (ContextHandler handler : ApiCenter.CONTEXTS) {
            handlers[index++] = handler;
        }
        contexts.setHandlers(handlers);
        server.setHandler(contexts);
    }
}
