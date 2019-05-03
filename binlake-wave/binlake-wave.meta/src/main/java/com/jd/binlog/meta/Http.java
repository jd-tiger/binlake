package com.jd.binlog.meta;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * http class is used to identify http request and response
 *
 * @author pengan
 */
public class Http {
    /**
     * kill request for http
     * in relation to /kill
     *
     * @author pengan
     */
    public static class KillRequest {
        private String key; // key 带上create time的zookeeper 标志
        private long leaderVersion; // dump的版本号

        public KillRequest(String key, long leaderVersion) {
            this.key = key;
            this.leaderVersion = leaderVersion;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getLeaderVersion() {
            return leaderVersion;
        }

        public void setLeaderVersion(long leaderVersion) {
            this.leaderVersion = leaderVersion;
        }

        public static byte[] marshal(Http.KillRequest req) throws IOException {
            return new ObjectMapper().writeValueAsBytes(req);
        }

        public static Http.KillRequest unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(json, Http.KillRequest.class);
        }
    }

    /**
     * selector request: leader selector request
     *
     * @author pengan
     */
    public static class RefreshRequest {
        String key;             // zookeeper 节点对应的key值 = mysql{host:port}
        long leaderVersion;       // take leader version
        long metaVersion;        // 元数据版本
        String leader;          // newly leader
        List<String> candidate; // newly candidate

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public long getLeaderVersion() {
            return leaderVersion;
        }

        public void setLeaderVersion(long leaderVersion) {
            this.leaderVersion = leaderVersion;
        }

        public void setCandidate(List<String> candidate) {
            this.candidate = candidate;
        }

        public void setLeader(String leader) {
            this.leader = leader;
        }

        public List<String> getCandidate() {
            return candidate;
        }

        public String getLeader() {
            return leader;
        }

        public long getMetaVersion() {
            return metaVersion;
        }

        public void setMetaVersion(long metaVersion) {
            this.metaVersion = metaVersion;
        }

        public static byte[] marshal(RefreshRequest req) throws Exception {
            return new ObjectMapper().writeValueAsBytes(req);
        }

        public static RefreshRequest unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(json, RefreshRequest.class);
        }

        public static void main(byte[] args) throws Exception {
            RefreshRequest req = new RefreshRequest();
            List<String> can = new LinkedList<>();
            can.addAll(Arrays.asList(new String[]{"127.0.0.1", "127.0.0.2", "127.0.0.3"}));
            req.setCandidate(can);
            req.setLeader("127.0.0.3");
            req.setKey("localhost:3358");

            System.err.println(new String(RefreshRequest.marshal(req)));

            System.err.println(new String(new ObjectMapper().writeValueAsBytes(can)));
        }
    }

    /**
     * response status
     */
    public enum Response {
        SUCCESS, FAILURE;

        public static Http.Response unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(json, Http.Response.class);
        }

        public static byte[] marshal(Http.Response req) throws Exception {
            return new ObjectMapper().writeValueAsBytes(req);
        }
    }

    public static void main(String[] args) throws Exception {
        Http.Response resp = Response.SUCCESS;

        byte[] bts = Http.Response.marshal(resp);

        System.err.println(new String(bts));


        System.err.println(Http.Response.unmarshalJson(bts));
    }
}
