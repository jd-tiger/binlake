package com.jd.binlake.tower.api;



/**
 * Created by pengan on 17-3-3.
 */
public class Constants {
    public static final int SUCCESS = 1000;
    public static final int FAILURE = 1;
    static final int ZK_NODE_CREATE_FAILURE = 102;
    static final int ZK_NODE_UPDATE_FAILURE = 103;
    static final int ZK_QUERY_FAILURE = 104;
    static final int UPDATE_INSTANCE_FAILURE = 113;
    static final int INSTANCE_NOT_EXIST = 115;
    static final int BIND_LEADERS_FAILURE = 118;
    static final int UPDATE_CANDIDATE_FAILURE = 126;
    static final int PROTOBUF_PARSE_ERROR = 127;
    static final int JSON_PARSE_ERROR = 128;

    public static final String EMPTY = "";
    public static final String ZNODE_SEPARATOR = ":";
}
