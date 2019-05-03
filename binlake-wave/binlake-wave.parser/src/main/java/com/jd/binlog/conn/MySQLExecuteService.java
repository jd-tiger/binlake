package com.jd.binlog.conn;

import com.jd.binlog.util.ExecutorUtils;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created on 18-5-10
 *
 * @author pengan
 */
abstract class MySQLExecuteService {
    /**
     * used for Create mysql connection and query on MySQL
     */
    static final ThreadPoolExecutor connExecutor = ExecutorUtils.create("connection", 4);
}
