package com.jd.binlog.util;

import org.apache.log4j.Logger;

/**
 * Created on 18-7-18
 *
 * @author pengan
 */
public interface LogUtils {
    Logger info = Logger.getLogger("infoLogger");
    Logger debug = Logger.getLogger("debugLogger");
    Logger error = Logger.getLogger("errorLogger");
    Logger warn = Logger.getLogger("warnLogger");
}
