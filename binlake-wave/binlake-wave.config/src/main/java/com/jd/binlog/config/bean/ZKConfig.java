package com.jd.binlog.config.bean;

import com.jd.binlog.config.validate.Validation;

/**
 * Created by pengan on 16-12-27.
 */
public class ZKConfig implements Validation {
    private String servers;
    private String metaPath;
    private String binlogKey = "/dynamic";
    private String counterPath = "/counter";
    private String terminalPath = "/terminal";
    private String candidatePath = "/candidate";
    private String leaderPath = "/leader";
    private int retryTimes = 10;
    private int sleepMsBetweenRetries = 1000;

    public String getTerminalPath() {
        return terminalPath;
    }

    public String getServers() {
        return servers;
    }

    public String getMetaPath() {
        return metaPath;
    }

    public String getBinlogKey() {
        return binlogKey;
    }

    public String getCounterPath() {
        return counterPath;
    }

    public String getLeaderPath() {
        return leaderPath;
    }

    public String getCandidatePath() {
        return candidatePath;
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
