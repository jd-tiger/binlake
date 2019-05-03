package com.jd.binlog.dbsync;


import java.util.LinkedList;

/**
 * Created by pengan on 17-6-4.
 *
 * one uuid  many ranges
 *
 * 9583d493-ce62-11e6-91a1-507b9d578e91:1-4:6-9:12-15
 */
public class GTID {
    public LinkedList<ClosedInterval> ranges;
    public String sid;     // uuid

    public GTID(long gtidGNO, String sid) {
        this.sid = sid.trim();
        this.ranges = new LinkedList<ClosedInterval>();
        ClosedInterval tr = new ClosedInterval(gtidGNO);
        this.ranges.add(tr);
    }

    public GTID(String sid) {
        this.sid = sid.trim();
        this.ranges = new LinkedList<ClosedInterval>();
    }

    public void addInterval(ClosedInterval interval) {
        this.ranges.add(interval);
    }


    /**
     * 同gtid - uuid 合并
     *
     * @param pre
     */
    public void mergeGtid(GTID pre) {
        ClosedInterval last = pre.ranges.removeLast();
        ClosedInterval first = ranges.pop();
        if (first.merge(last)) {
            // 合并成功

            LinkedList<ClosedInterval> temp = new LinkedList<ClosedInterval>();
            temp.addAll(pre.ranges);
            temp.add(first);
            temp.addAll(ranges);

            this.ranges.clear();
            this.ranges.addAll(temp);

        } else {
            LinkedList<ClosedInterval> temp = new LinkedList<ClosedInterval>();
            temp.addAll(pre.ranges);
            temp.add(last);
            temp.add(first);
            temp.addAll(ranges);

            this.ranges.clear();
            this.ranges.addAll(temp);

        }

        pre.ranges.add(last);
    }


    @Override
    public String toString() {
        StringBuilder gtid = new StringBuilder(sid).append(":");
        for (ClosedInterval interval : ranges) {
            gtid.append(interval);
            gtid.append(":");
        }
        if (ranges.size() != 0) {
            gtid.setLength(gtid.length() - 1);
        }
        return gtid.toString();
    }

    public GTID deepCopy() {
        GTID gtid = new GTID(sid);
        for (ClosedInterval interval : ranges) {
            gtid.addInterval(interval.deepCopy());
        }
        return gtid;
    }


    /**
     * 解析gtid
     *
     * @param sids
     * @return
     */
    public static GTID parseGTID(String[] sids) {
        GTID gtid = new GTID(sids[0].trim());

        for (int index = 1; index < sids.length; index++) {
            String[] inters = sids[index].split("-");

            if (inters.length == 1) {
                gtid.addInterval(new GTID.ClosedInterval(Long.parseLong(inters[0])));
            } else {
                gtid.addInterval(new GTID.ClosedInterval(Long.parseLong(inters[0]),
                        Long.parseLong(inters[1])));
            }
        }
        return gtid;
    }

    /**
     * 只考虑闭区间情况
     */
    public static class ClosedInterval {
        long lower;
        long upper;

        public ClosedInterval(long gtidGNO) {
            this.lower = gtidGNO;
            this.upper = gtidGNO;
        }

        public ClosedInterval(long lower, long upper) {
            this.lower = lower;
            this.upper = upper;
        }

        /**
         * pre transaction range merged to current transaction range
         *
         * @param pre
         * @return
         */
        public boolean merge(ClosedInterval pre) {
            if (pre.upper + 1 == lower || pre.upper == lower) {
                this.lower = pre.lower;
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            if (lower == upper) {
                return lower + "";
            }
            return lower + "-" + upper;
        }

        public ClosedInterval deepCopy() {
            return new ClosedInterval(lower, upper);
        }
    }
}
