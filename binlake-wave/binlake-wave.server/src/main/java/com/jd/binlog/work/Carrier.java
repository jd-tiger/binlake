package com.jd.binlog.work;

import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.LogPosition;
import com.jd.binlog.dbsync.event.RowsLogBuffer;
import com.jd.binlog.dbsync.event.RowsLogEvent;
import com.jd.binlog.dbsync.event.TableMapLogEvent;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.parser.TableMeta;
import com.jd.binlog.util.LogUtils;

import java.util.*;

/**
 * 消息载体
 * <p>
 * Created by pengan on 17-5-25.
 */
public class Carrier implements IMessage {
    /**
     * 这些部分一旦初始化 不再变更
     */
    static class ReadOnly {
        /**
         * 行数计数器
         */
        private int rowNumCounter = 0;

        //////////////////////////////只读数据 <b>start</b> 一旦初始化 后续就不会变更 //////////////////////////////////////

        // 是否是commit 用于truncate log position
        public boolean isCommit;

        // gtid no
        public long gtidGNO;

        // gtid
        public String sid; // gtid event sid;

        // binlog file
        public String binlogFile;

        // binlog position
        public LogPosition logPos;

        // event type
        public int eventType;

        // log event
        public LogEvent logEvent;

        // 对应元数据
        public TableMeta tableMeta;

        // 库名
        public String db;

        // 表名
        public String table;

        // 实际的列数量
        public int realColNum;

        // MySQL host:port
        public String host;

        // transaction id only
        public long trxID;

        // host bytes
        public byte[] hostBytes;

        // java 类型
        public ArrayList<Integer> javaType;

        // 转换之后java 数据类型 避免客户端转换出错
        public ArrayList<Integer> cJavaType;

        // 列值 是否为空
        public List<Boolean[]> isNull;

        // is updated 按照完成的update 一整行来处理
        public List<Boolean[]> isUpdate;

        // 列值 因为可能为空 所以用数组
        public List<String[]> rowsVals;

        //////////////////////////////只读数据 <b>end</b> 一旦初始化 后续就不会变更 //////////////////////////////////////

        public ReadOnly duplicate() {
            ReadOnly nr = new ReadOnly();
            nr.rowNumCounter = rowNumCounter;
            nr.isCommit = isCommit;
            nr.gtidGNO = gtidGNO;
            nr.sid = sid;
            nr.binlogFile = binlogFile;
            nr.logPos = logPos;
            nr.eventType = eventType;
            nr.logEvent = logEvent;
            nr.tableMeta = tableMeta;
            nr.db = db;
            nr.table = table;
            nr.realColNum = realColNum;
            nr.host = host;
            nr.trxID = trxID;
            nr.hostBytes = hostBytes;
            nr.javaType = javaType;
            nr.cJavaType = cJavaType;
            nr.isNull = isNull;
            nr.isUpdate = isUpdate;
            nr.rowsVals = rowsVals;
            return nr;
        }

        /**
         * clear reference
         */
        public void clearReference() {
            binlogFile = null;
            logPos = null;
            logEvent = null;
            tableMeta = null;
            db = null;
            table = null;
            javaType = null;
            cJavaType = null;
            isNull = null;
            isUpdate = null;
            rowsVals = null;
            host = null;
            hostBytes = null;

            trxID = 0;
        }


        public void clear() {
            if (javaType != null) {
                javaType.clear();
            }

            if (cJavaType != null) {
                cJavaType.clear();
            }

            if (isNull != null) {
                isNull.clear();
            }

            if (isUpdate != null) {
                isUpdate.clear();
            }

            if (rowsVals != null) {
                rowsVals.clear();
            }
        }
    }

    public ReadOnly car;

    /**
     * 位图表示遗留下来的数据
     */
    private BitSet reservedCol;

    /**
     * 伪列信息 需要去重
     */
    private Map<String, String> fakeCols;

    /**
     * 已经读取的行标 需要自己来判断 update
     */
    private int readRowIndex;

    /**
     * 业务主键 列位图
     */
    private BitSet keyCol;

    public Carrier(String binlogFile, String host, LogEvent event) {
        car = new ReadOnly();
        car.isCommit = false;
        car.binlogFile = binlogFile;
        car.host = host;
        car.hostBytes = host.getBytes(); // 只拷贝一份
        car.logEvent = event;
        car.eventType = event.getHeader().getType();
    }

    // 空构造函数
    public Carrier() {
    }

    @Override
    public void clearReference() {
        if (this.keyCol != null) {
            this.keyCol.clear();
            this.keyCol = null;
        }

        if (this.fakeCols != null) {
            this.fakeCols.clear();
            this.fakeCols = null;
        }

        if (reservedCol != null) {
            this.reservedCol.clear();
            this.reservedCol = null;
        }
        car.clearReference();
    }

    @Override
    public void clear() {
        car.clear();
    }

    @Override
    public boolean isCommitEvent() {
        LogPosition pos = car.logPos;
        if (pos != null) {
            return pos.isCommit();
        }
        return false;
    }

    @Override
    public LogPosition getLogPosition() {
        return car.logPos;
    }

    @Override
    public String getDb() {
        return car.db;
    }

    @Override
    public String getHost() {
        return car.host;
    }

    @Override
    public byte[] getHostBytes() {
        return car.hostBytes;
    }

    @Override
    public long getTrxID() {
        return car.trxID;
    }

    @Override
    public int getEventType() {
        return car.eventType; // 多处使用
    }

    @Override
    public String getTable() {
        if (car.db == null || car.table == null) {
            return null;
        }

        return car.db + "." + car.table;
    }

    @Override
    public void applyKeyBitSet(BitSet keyBs) {
        if (keyCol == null) {
            this.keyCol = keyBs;
            return;
        }

        // 如果匹配上多个规则 取并集
        this.keyCol.or(keyBs);
    }

    @Override
    public BitSet getKeyBitSet() {
        return keyCol;
    }

    @Override
    public String[] currRowVals() {
        return car.rowsVals.get(readRowIndex);
    }

    @Override
    public List<String> getColumns() {
        return car.tableMeta.columns;
    }

    @Override
    public List<String> getUpperColumns() {
        return car.tableMeta.upColumns;
    }

    @Override
    public int getRowNumCounter() {
        return car.rowNumCounter;
    }

    @Override
    public void addFakeColumn(List<Meta.Pair> fakeCols, BitSet bs) {
        if (this.reservedCol == null) {
            this.reservedCol = bs;
            // 初始化 fake columns
            this.fakeCols = new HashMap<String, String>();
            for (Meta.Pair pair : fakeCols) {
                this.fakeCols.put(pair.getKey(), pair.getValue());
            }
            return;
        }

        this.reservedCol.and(bs); // 过滤列取 交集
        for (Meta.Pair pair : fakeCols) { // 增加伪列 取并集
            this.fakeCols.put(pair.getKey(), pair.getValue());
        }
    }

    @Override
    public String getBinlogFile() {
        return car.binlogFile;
    }

    @Override
    public LogEvent getLogEvent() {
        return car.logEvent;
    }

    @Override
    public TableMeta getTableMeta() {
        return car.tableMeta;
    }

    @Override
    public void setGTID(String sid, long gtidGNO) {
        car.sid = sid;
        car.gtidGNO = gtidGNO;
    }

    @Override
    public void initMeta() {
        TableMapLogEvent table = ((RowsLogEvent) car.logEvent).getTable();
        int cnt = table.getColumnCnt();
        car.realColNum = cnt; // 实际的列数量

        int metaColNum = car.tableMeta.getColumnSize();

        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug(table.getDbName() + "." + table.getTableName() + ",table_id:" +
                    table.getTableId() + ", real column num:" + car.realColNum + ",meta column num:" + metaColNum);
        }

        // 补齐 多余的列信息  由于binlog 列数量已经不可变更 所以在实际操作当中必须以 binlog数量为准
        if (cnt > metaColNum) { // 如果binlog 當中列數量 比實際的列數量要少
            // 列被删除了
            // 所以需要增加列信息
            // 需要拷贝老的tableMeta 但是不改变外层
            car.tableMeta = car.tableMeta.duplicateAndAddColumn(cnt);
        }

        TableMapLogEvent.ColumnInfo[] cis = table.getColumnInfo();

        // 数组长度  只能拷贝 否则会因为 元数据变更导致内存被清除
        car.javaType = new ArrayList<>(cnt);
        car.cJavaType = new ArrayList<>(cnt);

        // 处理元数据信息
        List<Boolean> isBinary = car.tableMeta.isBinary;
        for (int i = 0; i < cnt; i++) {
            // 处理列信息
            int jt = RowsLogBuffer.mysqlToJavaType(cis[i].type, cis[i].meta, isBinary.get(i));
            car.javaType.add(jt);
            car.cJavaType.add(jt);
        }

        car.rowsVals = new ArrayList<String[]>();
        car.isNull = new ArrayList<Boolean[]>();

        switch (car.eventType) {
            case LogEvent.UPDATE_ROWS_EVENT:
            case LogEvent.UPDATE_ROWS_EVENT_V1:
                car.isUpdate = new ArrayList<Boolean[]>();
                break;
        }
    }

    @Override
    public List<Integer> getJavaType() {
        return car.javaType;
    }

    @Override
    public List<Integer> getCJavaType() {
        return car.cJavaType;
    }

    @Override
    public String[] getPreRowVal() {
        return car.rowsVals.get(car.rowNumCounter - 1);
    }

    @Override
    public String[] getNewRowVal() {
        car.rowNumCounter++;
        String[] row = new String[car.realColNum];
        car.rowsVals.add(row);
        return row;
    }

    @Override
    public Boolean[] getIsNullNewRow() {
        Boolean[] nul = new Boolean[car.realColNum];
        Arrays.setAll(nul, (index) -> false);
        car.isNull.add(nul);
        return nul;
    }

    @Override
    public Boolean[] getIsUpdatedNewRow() {
        Boolean[] updated = new Boolean[car.realColNum];
        Arrays.setAll(updated, (index) -> false);
        car.isUpdate.add(updated);
        return updated;
    }

    @Override
    public IMessage duplicate() {
        Carrier nc = new Carrier();
        nc.car = car.duplicate();
        return nc;
    }

    @Override
    public Boolean[] getIsNull(int row) {
        return car.isNull.get(row);
    }

    @Override
    public Boolean[] getIsUpdated(int row) {
        return car.isUpdate.get(row);
    }

    @Override
    public String[] getRowVals(int row) {
        readRowIndex = row; // 记录当前读取的行标
        return car.rowsVals.get(row);
    }

    @Override
    public Map<String, String> getFakeCols() {
        return fakeCols;
    }

    public BitSet getReservedCol() {
        return reservedCol;
    }
}
