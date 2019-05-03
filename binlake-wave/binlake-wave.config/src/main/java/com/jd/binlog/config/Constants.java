package com.jd.binlog.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by pengan on 16-12-27.
 * <p>
 * packet private configuration
 */
class Constants {
    /**
     * zookeeper configuration
     */
    private static final String ZK_PATH_META = "zk.path.meta";
    private static final String ZK_SERVERS = "zk.servers";

    /**
     * wave servers configuration
     */
    private static final String WAVE_PROCESSORS = "wave.server.processors";
    private static final String WAVE_THROTTLE_SIZE = "wave.server.throttle.size";
    private static final String WAVE_DUMP_LATCH = "wave.dump.latch";
    private static final String WAVE_KILL_LATCH = "wave.kill.latch";
    private static final String WAVE_TIMER_PERIOD = "wave.timer.period";

    /**
     * wave rpc service port
     */
    private static final String WAVE_HTTP_SERVER_PORT = "wave.http.server.port";

    private static final String WAVE_AGENT_SERVER_PORT = "wave.agent.server.port";


    static Map<String, String> ZK_FIELDS = new LinkedHashMap<>();
    static Map<String, String> ZK_FIELDS_DEFAULT_VALUE = new LinkedHashMap<>();

    static {
        ZK_FIELDS.put(ZK_PATH_META, "metaPath");
        ZK_FIELDS.put(ZK_SERVERS, "servers");

        ZK_FIELDS_DEFAULT_VALUE.put(ZK_PATH_META, "/zk/wave");
        ZK_FIELDS_DEFAULT_VALUE.put(ZK_SERVERS, "127.0.0.1:2181");
    }

    static Map<String, String> SERVER_FIELDS = new LinkedHashMap<>();
    static Map<String, String> SERVER_FIELDS_DEFAULT_VALUE = new LinkedHashMap<>();

    static {
        SERVER_FIELDS.put(WAVE_PROCESSORS, "processors");
        SERVER_FIELDS.put(WAVE_THROTTLE_SIZE, "throttleSize");
        SERVER_FIELDS.put(WAVE_HTTP_SERVER_PORT, "httpPort");
        SERVER_FIELDS.put(WAVE_AGENT_SERVER_PORT, "agentPort");
        SERVER_FIELDS.put(WAVE_DUMP_LATCH, "dumpLatch");
        SERVER_FIELDS.put(WAVE_KILL_LATCH, "killLatch");
        SERVER_FIELDS.put(WAVE_TIMER_PERIOD, "timerPeriod");

        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_PROCESSORS, "4"); // wave default processor number
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_THROTTLE_SIZE, "16");
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_HTTP_SERVER_PORT, "8083"); // wave http port
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_AGENT_SERVER_PORT, "4006"); // wave agent port
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_DUMP_LATCH, "6"); // dump latch
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_KILL_LATCH, "3"); // kill latch
        SERVER_FIELDS_DEFAULT_VALUE.put(WAVE_TIMER_PERIOD, "4000");
    }
}
