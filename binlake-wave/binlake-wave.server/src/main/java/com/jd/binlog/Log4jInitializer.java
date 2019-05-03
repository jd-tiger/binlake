package com.jd.binlog;

import org.apache.log4j.LogManager;
import org.apache.log4j.helpers.FileWatchdog;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by pengan on 16-9-8.
 */
public final class Log4jInitializer {

    private static final String format = "yyyy-MM-dd HH:mm:ss";

    public static void configureAndWatch(long delay) {
        String home = System.getProperty("wave.home");

        if (home == null) {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            LogLog.warn(sdf.format(new Date()) + " [wave.home] is not set.");
        } else {
            XMLWatchdog xdog = new XMLWatchdog(home + File.separator + "/conf/log4j.xml");
            xdog.setDelay(1000); // 每10s 检查一次
            xdog.setName("Log4jWatchdog");
            xdog.setDaemon(false);
            xdog.setDelay(delay);
            xdog.start();
        }
    }

    private static final class XMLWatchdog extends FileWatchdog {

        public XMLWatchdog(String filename) {
            super(filename);
        }

        @Override
        public void doOnChange() {
            new DOMConfigurator().doConfigure(filename, LogManager.getLoggerRepository());
            LogLog.warn(new SimpleDateFormat(format).format(new Date()) + " [" + filename + "] load completed.");
        }
    }

}
