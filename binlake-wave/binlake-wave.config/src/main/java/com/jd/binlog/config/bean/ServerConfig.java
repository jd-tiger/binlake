package com.jd.binlog.config.bean;

import com.jd.binlog.config.validate.Validation;

/**
 * Created by pengan on 16-12-27.
 */
public class ServerConfig implements Validation {
    private int dumpLatch; // dump 分布式集群重试次数上线
    private int killLatch; // preleader kill 重复次数
    private int throttleSize; // 流控 buffer size
    private int processors; // PROCESSORS
    private int httpPort;  // HTTP PROT
    private int agentPort; // agent port
    private long timerPeriod;
    private String host;
    private String waveCluster;

    public String getWaveCluster() {
        return waveCluster;
    }

    public void setWaveCluster(String waveCluster) {
        this.waveCluster = waveCluster;
    }

    public int getProcessors() {
        return processors;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getHost() {
        return host;
    }

    public int getDumpLatch() {
        return dumpLatch;
    }

    public long getTimerPeriod() {
        return timerPeriod;
    }

    public int getKillLatch() {
        return killLatch;
    }

    public int getThrottleSize() {
        return throttleSize;
    }

    public int getAgentPort() {
        return agentPort;
    }

    public void validate() throws IllegalArgumentException {

    }
}
