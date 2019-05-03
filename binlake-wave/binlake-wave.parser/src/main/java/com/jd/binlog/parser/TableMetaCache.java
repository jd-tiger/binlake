package com.jd.binlog.parser;

import com.jd.binlog.conn.MySQLConnector;
import com.jd.binlog.conn.MySQLExecutor;
import com.jd.binlog.mysql.FieldPacket;
import com.jd.binlog.mysql.ResultSetPacket;
import com.jd.binlog.util.LogUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by pengan on 16-12-20.
 */
public class TableMetaCache {
    private static final String SHOW_TABLES = "SELECT concat('`', table_schema, '`.`', table_name, '`')\n" +
            "FROM information_schema.tables\n" +
            "WHERE UPPER(TABLE_SCHEMA) NOT IN ('TEST', 'MYSQL', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA');";
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String COLUMN_TYPE = "COLUMN_TYPE";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String COLUMN_KEY = "COLUMN_KEY";
    public static final String COLUMN_DEFAULT = "COLUMN_DEFAULT";
    public static final String EXTRA = "EXTRA";
    private MySQLConnector connector;

    // 第一层tableId,第二层schema.table,解决tableId重复，对应多张表
    private Map<String, TableMeta> cache;

    public TableMetaCache(final MySQLConnector connector) throws IOException {
        this.connector = connector;
        this.connector.connect();
        cache = new LinkedHashMap<>();
    }

    /**
     * 更新指定表结构
     * 调用两次防止connector超期
     * 线程安全 ： 单线程调用
     *
     * @param schema
     * @param tableName
     */
    public TableMeta refreshTableCache(String schema, String tableName) {
        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("refreshTableCache: " + schema + "," + tableName);
        }
        TableMeta meta = getTableMeta(schema, tableName, false);
        cache.put(getFullName(schema, tableName), meta);
        return meta;
    }

    public TableMeta getTableMeta(String schema, String table, boolean useCache) {
        String fullName = getFullName(schema, table);

        if (useCache) {
            return cache.get(fullName);
        }
        // 注意这里不需要释放 对象的引用会自动的替换 然后其余的引用自动会解除
        cache.remove(fullName);

        try {
            return getTableMeta0(fullName);
        } catch (IOException e) {
            LogUtils.error.error("获取表结构 : " + fullName + " 失败 ", e);
            fullName = getFullName(schema, table);

            //retry one more time
            try {
                connector.reconnect();
                return getTableMeta0(fullName);
            } catch (IOException e1) {

                LogUtils.error.error(table + " update to " + table + " , 更改临时表名后, 获取表结构 : " + fullName + " 仍然失败 ", e1);
                return null;
            }
        }
    }

    /**
     * 单线程获取表结构
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    private synchronized TableMeta getTableMeta0(String tableName) throws IOException {
        if (LogUtils.debug.isDebugEnabled()) {
            LogUtils.debug.debug("getTableMeta0:" + "desc " + tableName);
        }
        TableMeta tableMeta = null;

        if ((tableMeta = cache.get(tableName)) != null) {
            return tableMeta;
        }
        MySQLExecutor executor = new MySQLExecutor(connector);
        ResultSetPacket packet = executor.query("desc " + tableName);
        return new TableMeta(tableName, parserTableMeta(packet));
    }

    private ArrayList<TableMeta.FieldMeta> parserTableMeta(ResultSetPacket packet) {
        Map<String, Integer> nameMaps = new HashMap<String, Integer>(6, 1f);
        int index = 0;

        for (FieldPacket fieldPacket : packet.getFields()) {
            nameMaps.put(new String(fieldPacket.orgName), index++);
        }

        int size = packet.getFields().size();
        int count = packet.getFieldValues().size() / packet.getFields().size();
        ArrayList<TableMeta.FieldMeta> result = new ArrayList<TableMeta.FieldMeta>();

        for (int i = 0; i < count; i++) {
            TableMeta.FieldMeta meta = new TableMeta.FieldMeta();
            // 做一个优化，使用String.intern()，共享String对象，减少内存使用
            meta.setColumnName(packet.getFieldValues().get(nameMaps.get(COLUMN_NAME) + i * size).intern());//you can read mysql packet protocol
            meta.setColumnType(packet.getFieldValues().get(nameMaps.get(COLUMN_TYPE) + i * size));
            meta.setIsNullable(packet.getFieldValues().get(nameMaps.get(IS_NULLABLE) + i * size));
            meta.setIsKey(packet.getFieldValues().get(nameMaps.get(COLUMN_KEY) + i * size));
            meta.setDefaultValue(packet.getFieldValues().get(nameMaps.get(COLUMN_DEFAULT) + i * size));
            meta.setExtra(packet.getFieldValues().get(nameMaps.get(EXTRA) + i * size));
            result.add(meta);
        }
        return result;
    }

    private String getFullName(String schema, String table) {
        StringBuilder builder = new StringBuilder();
        return builder.append('`')
                .append(schema)
                .append('`')
                .append('.')
                .append('`')
                .append(table)
                .append('`')
                .toString();
    }

    public void kill(long connId) throws IOException {
        MySQLExecutor executor = new MySQLExecutor(connector);
        executor.update("KILL CONNECTION " + connId);
    }

    public void close() throws IOException {
        cache.clear();

        MySQLConnector connector = this.connector;
        if (connector != null) {
            connector.disconnect();
        }
    }
}
