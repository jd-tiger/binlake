package com.jd.binlog.dump;

/**
 * Created by pengan on 17-2-28.
 */
public enum DumpType {
    COM_BINLOG_DUMP(0),
    COM_BINLOG_DUMP_GTID(1),
    OTHER(5);
    private int code;

    DumpType(int index) {
        this.code = index;
    }

    public int getCode() {
        return code;
    }
}
