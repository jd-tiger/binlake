package com.jd.binlog.mysql;

/**
 * Created by ninet on 17-3-1.
 */
public class GTIDs {
    private String sid;
    private long gnoStart;
    private long gnoEnd;

    public GTIDs(String sid, long gnoStart, long gnoEnd) {
        this.sid = sid;
        this.gnoStart = gnoStart;
        this.gnoEnd = gnoEnd;
    }

    public String getSid() {
        return this.sid;
    }

    public long getGnoStart() {
        return  this.gnoStart;
    }

    public long getGnoEnd() {
        return this.gnoEnd;
    }

    public boolean equalSid(String sid) {
        if (this.sid.equals(sid)) {
            return true;
        } else {
            return false;
        }
    }

    public boolean inGnoSets(long gnoL, long gnoR) {
        if (gnoL >= this.gnoStart && gnoR <= this.gnoEnd) {
            return true;
        } else {
            return false;
        }
    }
}