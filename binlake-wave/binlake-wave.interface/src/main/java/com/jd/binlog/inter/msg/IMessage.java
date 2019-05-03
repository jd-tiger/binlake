package com.jd.binlog.inter.msg;

import com.jd.binlog.dbsync.LogEvent;
import com.jd.binlog.dbsync.LogPosition;
import com.jd.binlog.meta.Meta;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created on 18-5-14
 *
 * @author pengan
 */
public interface IMessage {
    /**
     * 获取 消息体 的host 信息
     */
    String getHost();

    /**
     * 获取host bytes
     */
    byte[] getHostBytes();

    /**
     * 获取 transaction id
     */
    long getTrxID();

    /**
     * 获取事件
     *
     * @return
     */
    int getEventType();

    /**
     * 获取表名全程
     *
     * @return
     */
    String getTable();

    /**
     * 添加伪列信息
     *
     * @param fakeCols
     * @param bs
     */
    void addFakeColumn(List<Meta.Pair> fakeCols, BitSet bs);

    /**
     * 获取binlog 文件
     *
     * @return
     */
    String getBinlogFile();

    /**
     * 获取binlog 事件
     *
     * @return
     */
    LogEvent getLogEvent();

    /**
     * get table meta
     *
     * @return
     */
    com.jd.binlog.parser.TableMeta getTableMeta();

    /**
     * set gtid sid
     *
     * @param sid
     */
    void setGTID(String sid, long gtidGNO);

    /**
     * 将table meta 信息转换成数组
     *
     * @return
     */
    void initMeta();

    /**
     * 获取所有列类型 MySQL type 转成 java type
     *
     * @return
     */
    List<Integer> getJavaType();

    /**
     * 获取转换之后的java 类型
     *
     * @return
     */
    List<Integer> getCJavaType();

    /**
     * 获取前一行的数据 用来判断update 字段
     *
     * @return
     */
    String[] getPreRowVal();

    /**
     * 获取新行 值数组 需要添加到 rowsVal list当中
     *
     * @return
     */
    String[] getNewRowVal();

    /**
     * 获取新行 isnull 数组 添加到 isNull list 当中
     *
     * @return
     */
    Boolean[] getIsNullNewRow();

    /**
     * 获取新行 isUpdated 数组 添加到 isUpdate  list当中
     *
     * @return
     */
    Boolean[] getIsUpdatedNewRow();

    /**
     * 复制 非只读成员变量
     */
    IMessage duplicate();

    /**
     * 获取 当前行列值 是否为空
     */
    Boolean[] getIsNull(int row);

    /**
     * 获取 列 是否是更新字段
     */
    Boolean[] getIsUpdated(int row);

    /**
     * 获取 单行 的数值
     */
    String[] getRowVals(int row);

    /**
     * 设置 业务主键列 位图
     */
    void applyKeyBitSet(BitSet keyBs);

    /**
     * 获取 业务主键 列位图
     *
     * @return
     */
    BitSet getKeyBitSet();

    /**
     * 获取当前读取行数据
     *
     * @return
     */
    String[] currRowVals();

    /**
     * 获取列名 大写
     *
     * @return
     */
    List<String> getUpperColumns();

    /**
     * 返回行数计数器
     *
     * @return
     */
    int getRowNumCounter();


    /**
     * 获取保留列的位图
     *
     * @return
     */
    Map<String, String> getFakeCols();


    /**
     * 获取保留的列信息
     *
     * @return
     */
    BitSet getReservedCol();

    /**
     * clear reference 清除引用
     */
    void clearReference();

    /**
     * 清空 所有值
     */
    void clear();

    /**
     * 当前事件 是否是 commit 事件
     *
     * @return
     */
    boolean isCommitEvent();

    /**
     * 获取当前 事件对应的binlog 位置
     *
     * @return
     */
    LogPosition getLogPosition();

    /**
     * 获取库名
     *
     * @return
     */
    String getDb();

    /**
     * 获取 列信息
     *
     * @return
     */
    List<String> getColumns();


    /**
     * 日志统计安排: 整个过程分为如下步骤
     * dump delay: dump time - binlog.when()
     * decode delay: decode time - binlog.when()
     * parse delay: parse time - binlog.when()
     * convert delay: convert time - binlog.when()
     * send delay: send time - binlog.when()
     */
}
