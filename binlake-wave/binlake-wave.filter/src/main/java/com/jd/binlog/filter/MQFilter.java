package com.jd.binlog.filter;

import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.filter.aviater.AviaterRegexFilter;
import com.jd.binlog.inter.filter.IEventFilter;
import com.jd.binlog.inter.filter.IFilter;
import com.jd.binlog.meta.Meta;

import java.util.*;

/**
 * Created on 18-5-14
 *
 * @author pengan
 */
public class MQFilter implements IFilter {
    /**
     * table filter
     */
    TableFilter tf;

    /**
     * event type filter
     */
    EventTypeFilter evf;

    /**
     * 白名单列过滤器: 表示列如果属于白名单 则需要将列保留
     */
    ColumnFilter wcf = null;

    /**
     * 黑名单过滤器: 表示列如果属于黑名单 则需要过滤
     */
    ColumnFilter bcf = null;

    /**
     * fake column
     */
    List<Meta.Pair> fakeCols;


    /**
     * hash keys :业务主键
     */
    Set<String> hashKeys = null;


    /**
     * 过滤规则 是否是白名单
     *
     * @param filter
     * @param isWhite
     */
    public MQFilter(Meta.Filter filter, boolean isWhite) {
        tf = new TableFilter(filter.getTable(), isWhite);

        List<Integer> logEvents = new ArrayList<Integer>(filter.getEventType().size());

        List<Meta.EventType> evs = filter.getEventType();
        for (int i = 0; i < evs.size(); i++) {
            for (int ev : metaEvType2LogEvent(evs.get(i))) {
                logEvents.add(ev);
            }
        }

        if (isWhite) { // 如果是白名单 则需要添加 补充的事件
            int[] supplys = new int[]{
                    LogEvent.GTID_LOG_EVENT,
                    LogEvent.RAND_EVENT,
                    LogEvent.INTVAR_EVENT,
                    LogEvent.USER_VAR_EVENT,
                    LogEvent.ROWS_QUERY_LOG_EVENT,
                    LogEvent.XID_EVENT};
            for (int i : supplys) {
                logEvents.add(i);
            }
        }

        evf = new EventTypeFilter(logEvents);

        List<Meta.Column> whiteCols = filter.getWhite();

        if (whiteCols.size() != 0) {
            StringBuilder col = new StringBuilder();
            for (int i = 0; i < whiteCols.size(); i++) {
                col.append(whiteCols.get(i).getName()).append("|");
            }

            col.setLength(col.length() - 1);
            wcf = new ColumnFilter(col.toString(), isWhite);
        }

        List<Meta.Column> blackCols = filter.getBlack();
        if (blackCols.size() != 0) {
            StringBuilder col = new StringBuilder();
            for (int i = 0; i < blackCols.size(); i++) {
                col.append(blackCols.get(i).getName()).append("|");
            }

            col.setLength(col.length() - 1);
            bcf = new ColumnFilter(col.toString(), isWhite);
        }

        this.fakeCols = filter.getFakeColumn();

        if (filter.getHashKey().size() != 0) {
            this.hashKeys = new HashSet<>();
            this.hashKeys.addAll(filter.getHashKey());
        }
    }

    /**
     * 过滤 事件类型
     *
     * @param eventType
     * @return
     */
    @Override
    public boolean filterEventType(int eventType) {
        return evf.filter(eventType);
    }

    /**
     * 过滤表名
     *
     * @param table
     * @return
     */
    @Override
    public boolean filterTable(String table) {
        return tf.filter(table);
    }

    /**
     *　默认返回false 保留字段
     *
     * @param col
     * @return true: 表示过滤, false: 表示保留
     */
    @Override
    public boolean filterColumn(String col) {
        if (wcf != null && wcf.filter(col)) {
            // 如果列已经匹配上白名单 表示不过滤
            return false;
        }

        if (bcf != null && bcf.filter(col)) {
            // 如果列匹配上黑名单　表示需要过滤　
            return true;
        }

        //　默认保留
        return false;
    }

    @Override
    public boolean haveColumnFilter() {
        return wcf != null || bcf != null;
    }

    /**
     * @return
     */
    @Override
    public List<Meta.Pair> getFakeCols() {
        return fakeCols;
    }

    public Set<String> getHashKeys() {
        return hashKeys;
    }

    @Override
    public boolean haveHashKeys() {
        return hashKeys != null;
    }


    /**
     * event type filter
     */
    public static class EventTypeFilter implements IEventFilter<Integer> {
        private BitSet bs;

        public EventTypeFilter(long[] words) {
            bs = BitSet.valueOf(words);
        }

        public EventTypeFilter(List<Integer> words) {
            bs = new BitSet();
            for (int wd : words) {
                bs.set(wd, true);
            }
        }

        @Override
        public boolean filter(Integer event) throws BinlogException {
            return bs.get(event);
        }
    }

    /**
     * table filter
     */
    public static class TableFilter implements IEventFilter<String> {

        private AviaterRegexFilter filter;

        public TableFilter(String reg, boolean isWhite) {
            // 白名单 如果是空字符串 返回 false 表示过滤, 黑名单:空字符串, 返回true 表示过滤
            filter = new AviaterRegexFilter(reg, !isWhite);
        }

        @Override
        public boolean filter(String table) throws BinlogException {
            return filter.filter(table);
        }
    }

    /**
     * column filter
     */
    public static class ColumnFilter implements IEventFilter<String> {
        private AviaterRegexFilter filter;

        public ColumnFilter(String reg, boolean isWhite) {
            filter = new AviaterRegexFilter(reg, isWhite);
        }

        @Override
        public boolean filter(String event) throws BinlogException {
            return filter.filter(event);
        }
    }

    /**
     * 事件转换工具
     *
     * @param et
     * @return
     */
    private int[] metaEvType2LogEvent(Meta.EventType et) {
        switch (et) {
            case TRUNCATE:
            case RENAME:
            case DINDEX:
            case ALTER:
            case QUERY:
            case CINDEX:
            case ERASE:
            case CREATE:
                return new int[]{LogEvent.QUERY_EVENT};
            case DELETE:
                return new int[]{LogEvent.DELETE_ROWS_EVENT, LogEvent.DELETE_ROWS_EVENT_V1};
            case INSERT:
                return new int[]{LogEvent.WRITE_ROWS_EVENT, LogEvent.WRITE_ROWS_EVENT_V1};
            case UPDATE:
                return new int[]{LogEvent.UPDATE_ROWS_EVENT, LogEvent.UPDATE_ROWS_EVENT_V1};
            default:
                return new int[]{};
        }
    }
}
