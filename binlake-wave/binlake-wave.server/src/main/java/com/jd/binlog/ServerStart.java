package com.jd.binlog;

import org.apache.log4j.Logger;

/**
 * Created by pengan on 16-12-15.
 * <p>
 * service start
 */
public class ServerStart {
    private static final Logger logger = Logger.getLogger(ServerStart.class);

    public static void main(String[] args) {
        Log4jInitializer.configureAndWatch(1000L);
        RealTimeInitializer.configureAndWatch(1000L);
        BinlogService service = BinlogService.getINSTANCE();
        try {
            service.start();
        } catch (Throwable e) {
            e.printStackTrace();
            logger.error(e);
            System.exit(-1);
        }
    }
}
