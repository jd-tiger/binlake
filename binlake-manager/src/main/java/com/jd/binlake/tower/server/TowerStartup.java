package com.jd.binlake.tower.server;

/**
 * Created by pengan on 17-3-2.
 * <p>
 * tower manager startup
 */
public class TowerStartup {
    public static void main(String[] args) {
        Log4jInitializer.configureAndWatch(1000L);
        TowerServer server = new TowerServer();

        try {
            server.start();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
