package com.jd.binlog.config.bean;

import com.jd.binlog.config.validate.Validation;

/**
 * Created by pengan on 16-12-27.
 */
public class ZKConfig implements Validation {
    private String servers;
    private String metaPath;
    private int retryTimes = 10;
    private int sleepMsBetweenRetries = 1000;

    public String getServers() {
        return servers;
    }

    public String getMetaPath() {
        return metaPath;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public int getSleepMsBetweenRetries() {
        return sleepMsBetweenRetries;
    }

    public void validate() throws IllegalArgumentException {
    }
}
