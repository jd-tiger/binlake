package com.jd.binlog.convert;

import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.event.RowsLogBuffer;
import com.jd.binlog.dbsync.event.RowsLogEvent;
import com.jd.binlog.dbsync.event.TableMapLogEvent;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.exception.ErrorCode;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.parser.TableMeta;
import com.jd.binlog.util.CharsetUtils;
import com.jd.binlog.util.LogUtils;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Types;
import java.util.BitSet;
import java.util.List;

/**
 * Created on 18-5-16
 *
 * @author pengan
 */
public class TableRowsParser {
    public static final String ISO_8859_1 = "ISO-8859-1";
    public static final int TINYINT_MAX_VALUE = 256;
    public static final int SMALLINT_MAX_VALUE = 65536;
    public static final int MEDIUMINT_MAX_VALUE = 16777216;
    public static final long INTEGER_MAX_VALUE = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE = new BigInteger("18446744073709551616");


    /**
     * parse 消息体
     *
     * @param msg
     * @throws UnsupportedEncodingException
     */
    public static void parse(IMessage msg) throws UnsupportedEncodingException {
        RowsLogEvent event = ((RowsLogEvent) msg.getLogEvent());
        TableMapLogEvent table = event.getTable();
        if (table == null) {
            // tableId对应的记录不存在
            throw new BinlogException(ErrorCode.WARN_MySQL_ROWEVENT_PARSE, new Exception("not found tableId:" + event.getTableId()), msg.getBinlogFile() + ":" + msg.getLogPosition());
        }

        msg.initMeta();

        RowsLogBuffer buffer = event.getRowsBuf(CharsetUtils.DEFAULT_CHARSET);

        BitSet cols = event.getColumns();
        BitSet changeCols = event.getChangeColumns();

        final int columnCnt = table.getColumnCnt();
        final TableMapLogEvent.ColumnInfo[] columnInfo = table.getColumnInfo();

        int eventType = msg.getEventType();
        while (buffer.nextOneRow(cols)) {

            if (LogUtils.debug.isDebugEnabled()) {
                LogUtils.debug.debug("event type " + eventType + ", row number " + msg.getRowNumCounter());
            }

            switch (eventType) {
                case LogEvent.WRITE_ROWS_EVENT:
                case LogEvent.WRITE_ROWS_EVENT_V1:
                    parseOneRow(columnCnt, false, msg, columnInfo, buffer);
                    break;
                case LogEvent.UPDATE_ROWS_EVENT:
                case LogEvent.UPDATE_ROWS_EVENT_V1:

                    // update需要处理before/after
                    parseOneRow(columnCnt, false, msg, columnInfo, buffer);
                    if (!buffer.nextOneRow(changeCols)) {
                        break;
                    }
                    parseOneRow(columnCnt, true, msg, columnInfo, buffer);
                    break;
                case LogEvent.DELETE_ROWS_EVENT:
                case LogEvent.DELETE_ROWS_EVENT_V1:
                    parseOneRow(columnCnt, false, msg, columnInfo, buffer);
                    break;
            }
        }
    }


    /**
     * 解析每一行 数据
     *
     * @param cnt        列数量
     * @param isUpdate   是否是update 的after 行
     * @param msg        消息体
     * @param columnInfo 列信息
     * @param buffer     rows log buffer
     * @throws UnsupportedEncodingException
     */
    private static void parseOneRow(int cnt, boolean isUpdate, IMessage msg,
                                    TableMapLogEvent.ColumnInfo[] columnInfo,
                                    RowsLogBuffer buffer) throws UnsupportedEncodingException {
        // 获取 java types
        List<Integer> jts = msg.getJavaType();

        // 获取转换之后的java 类型
        List<Integer> cjts = msg.getCJavaType();

        TableMeta meta = msg.getTableMeta();
        // is binary
        List<Boolean> isBinary = meta.isBinary;

        // is unsigned
        List<Boolean> isUnsigned = meta.isUnsigned;

        // is text
        List<Boolean> isText = meta.isTexts;

        // 前一个值 是否是update
        String[] preRowVal = null;
        Boolean[] isUpdated = null;
        if (isUpdate) {
            preRowVal = msg.getPreRowVal();
            isUpdated = msg.getIsUpdatedNewRow();
        }

        // 当前数组
        String[] currRowVal = msg.getNewRowVal();

        // 字段值是否为null
        Boolean[] isNull = msg.getIsNullNewRow();

        for (int i = 0; i < cnt; i++) {
            TableMapLogEvent.ColumnInfo info = columnInfo[i];

            // fixed issue
            // https://github.com/alibaba/canal/issues/66，特殊处理binary/varbinary，不能做编码处理
            buffer.nextValue(info.type, info.meta, isBinary.get(i));

            int javaType = jts.get(i);
            if (buffer.isNull()) {
                isNull[i] = true;
            } else {
                final Serializable value = buffer.getValue();
                // 处理各种类型
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        // 处理unsigned类型
                        Number number = (Number) value;
                        if (isUnsigned.get(i) && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1: /* MYSQL_TYPE_TINY */
                                    currRowVal[i] = String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE
                                            + number.intValue()));
                                    cjts.set(i, Types.SMALLINT); // 往上加一个量级
                                    break;

                                case 2: /* MYSQL_TYPE_SHORT */
                                    currRowVal[i] = String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE
                                            + number.intValue()));
                                    cjts.set(i, Types.INTEGER); // 往上加一个量级
                                    break;

                                case 3: /* MYSQL_TYPE_INT24 */
                                    currRowVal[i] = String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE
                                            + number.intValue()));
                                    cjts.set(i, Types.INTEGER); // 往上加一个量级
                                    break;

                                case 4: /* MYSQL_TYPE_LONG */
                                    currRowVal[i] = String.valueOf(Long.valueOf(INTEGER_MAX_VALUE
                                            + number.longValue()));
                                    cjts.set(i, Types.BIGINT); // 往上加一个量级
                                    break;

                                case 8: /* MYSQL_TYPE_LONGLONG */
                                    currRowVal[i] = BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue()))
                                            .toString();
                                    cjts.set(i, Types.DECIMAL); // 往上加一个量级，避免执行出错
                                    break;
                            }
                        } else {
                            // 对象为number类型，直接valueof即可
                            currRowVal[i] = String.valueOf(value);
                        }
                        break;
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                        // 对象为number类型，直接valueof即可
                        currRowVal[i] = String.valueOf(value);
                        break;
                    case Types.BIT:// bit
                        // 对象为number类型
                        currRowVal[i] = String.valueOf(value);
                        break;
                    case Types.DECIMAL:
                        currRowVal[i] = ((BigDecimal) value).toPlainString();
                        break;
                    case Types.TIMESTAMP:
                        // 修复时间边界值
                        // String v = value.toString();
                        // v = v.substring(0, v.length() - 2);
                        // columnBuilder.setValue(v);
                        // break;
                    case Types.TIME:
                    case Types.DATE:
                        // 需要处理year
                        currRowVal[i] = value.toString();
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // fixed text encoding
                        // https://github.com/AlibabaTech/canal/issues/18
                        // mysql binlog中blob/text都处理为blob类型，需要反查table
                        // meta，按编码解析text
                        if (isText.get(i)) {
                            currRowVal[i] = new String((byte[]) value);
                            cjts.set(i, Types.CLOB);
                        } else {
                            // byte数组，直接使用iso-8859-1保留对应编码，浪费内存
                            currRowVal[i] = new String((byte[]) value, ISO_8859_1);
                            cjts.set(i, Types.BLOB);
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        currRowVal[i] = value.toString();
                        break;
                    default:
                        currRowVal[i] = value.toString();
                }
            }
            // 设置是否update的标记位
            if (isUpdate) {
                isUpdated[i] = !StringUtils.equals(currRowVal[i], preRowVal[i]);
            }
        }
    }
}
