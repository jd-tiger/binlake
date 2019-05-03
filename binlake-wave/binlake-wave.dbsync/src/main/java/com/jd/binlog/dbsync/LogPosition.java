package com.jd.binlog.dbsync;

import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implements binlog position.
 *
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public class LogPosition implements Cloneable, Comparable<LogPosition> {
    private static final Logger logger = Logger.getLogger(LogPosition.class);

    /* binlog file's name */
    protected String fileName;

    /* position in file */
    protected long position;

    protected long when;

    protected boolean isCommit;

    /**
     * 引用计数器 用来判断是否可以将 log position  从队列当中清除
     */
    protected AtomicInteger counter;

    protected volatile String gtidSets;

    protected Map<String, GTID> gtids = new HashMap<String, GTID>(); // gtids 子集合 某些logPosition 合并gtid 得到的集合 注意 这儿肯定是顺序的

    public LogPosition(String fileName, final long position, long when, boolean isCommit) {
        this.fileName = fileName;
        this.position = position;
        this.when = when;
        this.isCommit = isCommit;
        this.counter = new AtomicInteger(1);
    }

    public LogPosition(String fileName, long position, long when, boolean isCommit, int counter) {
        this(fileName, position, when, isCommit);
        this.counter.set(counter);
    }

    /**
     * Binlog position copy init.
     */
    public LogPosition(LogPosition source) {
        this.fileName = source.fileName;
        this.position = source.position;
        this.when = source.when;
        this.counter = new AtomicInteger();
        this.counter.set(source.counter.get());
        this.isCommit = source.isCommit;

        if (source.gtids != null) {
            this.gtidSets = source.gtidSets;
            this.gtids = new LinkedHashMap<>();
            this.gtids.putAll(source.gtids);
        }
    }

    public final String getFileName() {
        return fileName;
    }

    public final long getPosition() {
        return position;
    }

    public long getWhen() {
        return when;
    }

    /* Clone binlog position without CloneNotSupportedException */
    public LogPosition clone() {
        return new LogPosition(this);
    }

    /**
     * Compares with the specified fileName and position.
     */
    public final int compareTo(String fileName, final long position) {
        final int val = this.fileName.compareTo(fileName);

        if (val == 0) {
            return (int) (this.position - position);
        }
        return val;
    }

    /**
     * {@inheritDoc}
     *
     * @see Comparable#compareTo(Object)
     */
    public int compareTo(LogPosition o) {
        final int val = fileName.compareTo(o.fileName);

        if (val == 0) {
            return (int) (position - o.position);
        }
        return val;
    }

    public boolean isCommit() {
        return isCommit;
    }

    /**
     * 增加引用计数器 可能有拆包 需要增加计数器
     */
    public void increment() {
        if (logger.isDebugEnabled()) {
            logger.debug("increment counter number is " + counter.get());
        }
        counter.incrementAndGet();
    }

    /**
     * 消息体已经发送 确定减少计数器 获取返回值
     *
     * @return {0} 表示该事件已经完全发送
     */
    public int decrementAndGet() {
        if (logger.isDebugEnabled()) {
            logger.debug("decrementAndGet counter number is " + counter.get());
        }
        return counter.decrementAndGet();
    }

    /**
     * {@inheritDoc}
     *
     * @see Object#equals(Object)
     */
    public boolean equals(Object obj) {
        if (obj instanceof LogPosition) {
            LogPosition pos = ((LogPosition) obj);
            return fileName.equals(pos.fileName)
                    && (this.position == pos.position);
        }
        return false;
    }

    public void addAllGtids(Map<String, GTID> gtids) {
        for (Map.Entry<String, GTID> gtid : gtids.entrySet()) {
            this.gtids.put(gtid.getKey(), gtid.getValue().deepCopy());
        }
    }

    public void addGtid(GTID newGtid) {
        String sid = newGtid.sid;
        if (this.gtids.containsKey(sid)) {
            GTID gtid = this.gtids.get(sid);
            newGtid.mergeGtid(gtid);
            this.gtids.put(sid, newGtid);
            return;
        }

        this.gtids.put(sid, newGtid);
    }

    /**
     * 刷新gtidset
     */
    public void refresh() {
        StringBuilder gtidStr = new StringBuilder("");
        for (Map.Entry<String, GTID> gtid : gtids.entrySet()) {
            gtidStr.append(gtid.getValue()).append(",");
        }
        if (gtidStr.length() > 1) {
            gtidStr.setLength(gtidStr.length() - 1);
        }
        this.gtidSets = gtidStr.toString();
    }

    /**
     * {@inheritDoc}
     *
     * @see Object#toString()
     */
    public String toString() {
        refresh();
        return fileName + ':' + position + ":" + gtidSets + ": " + isCommit + ":" + when + ":" + counter.get();
    }

    public String getGtidSets() {
        return gtidSets;
    }

    public boolean hasGTID() {
        return this.gtids.size() > 0;
    }

    public void mergeOriginLogPos(LogPosition originLogPos) {
        if (gtids.size() == 0) {
            gtids.putAll(originLogPos.gtids);
            return;
        }

        if (originLogPos.gtids.size() == 0) {
            return;
        }

        for (Map.Entry<String, GTID> gtid : originLogPos.gtids.entrySet()) {
            addOriginGtid(this.gtids, gtid.getValue());
        }
    }

    public void addOriginGtid(Map<String, GTID> gtids, GTID originGtid) {
        String sid = originGtid.sid;
        if (gtids.containsKey(sid)) {
            gtids.get(sid).mergeGtid(originGtid);
            return;
        }

        gtids.put(sid, originGtid);
    }

    public void clear() {
        gtids.clear();
        gtids = null;
    }

    public static void main(String[] args) {
        LogPosition logPos1 = new LogPosition("", 0, 0, false);
        GTID gtid = new GTID("6692c3d8-2a49-11e7-abef-00505681517d");
        gtid.addInterval(new GTID.ClosedInterval(1, 50007));

        GTID gtid4 = new GTID("ae5c3c5a-2a4d-11e7-ac0b-00505681029f");
        gtid4.addInterval(new GTID.ClosedInterval(1, 99));

        GTID gtid5 = new GTID("6692c3d8-2a49-11e7-abef-00505681517d");
        gtid5.addInterval(new GTID.ClosedInterval(50008, 50099));

        logPos1.addGtid(gtid);
        logPos1.addGtid(gtid4);
        logPos1.addGtid(gtid5);

        logPos1.refresh();
        System.err.println("pos1 : " + logPos1.getGtidSets());


        LogPosition pos2 = new LogPosition("", 0, 0, false);
        GTID gtid1 = new GTID("ae5c3c5a-2a4d-11e7-ac0b-00505681029f");
        gtid1.addInterval(new GTID.ClosedInterval(100, 2021));
        pos2.addGtid(gtid1);

        pos2.mergeOriginLogPos(logPos1);

        pos2.refresh();
        System.err.println("pos2 : " + pos2.getGtidSets());
////
////        GTID gtid3 = new GTID(3, "ae5c3c5a-2a4d-11e7-ac0b-00505681029f");
////        LogPosition logPos3 = new LogPosition("", 0, 0, false);
////
////        logPos3.addGTID(gtid3);
////
////        logPos3.merge(pos2);
////        logPos3.refresh();
////        System.err.println("pos3 : " + logPos3.getGtidSets());
//
//
//
    }
}
