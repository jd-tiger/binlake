package com.jd.binlog.parser;

import com.jd.binlog.util.CharUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;

/**
 * Created by pengan on 16-12-19.
 */
public class TableMeta {

    // column size
    private int columnSize;

    private String fullName; // schema.table

    /**
     * 固定信息 防止内存多次拷贝
     */

    // 列名
    public ArrayList<String> columns;

    // 列名 大写
    public ArrayList<String> upColumns;

    // 是否是主键
    public ArrayList<Boolean> isKey;

    // MySQL 当中数据类型
    public ArrayList<String> mySQLType;

    // 字段是否unsigned
    public ArrayList<Boolean> isUnsigned;

    // 字段是否为 binary
    public ArrayList<Boolean> isBinary;

    // 字段是否为 text 类型
    public ArrayList<Boolean> isTexts;

    public TableMeta() {
    }

    public TableMeta(String fullName, ArrayList<FieldMeta> fields) {
        this.fullName = fullName;
        this.columnSize = fields.size();

        columns = new ArrayList<>(columnSize);
        upColumns = new ArrayList<>(columnSize);
        isKey = new ArrayList<>(columnSize);
        mySQLType = new ArrayList<>(columnSize);
        isUnsigned = new ArrayList<>(columnSize);
        isBinary = new ArrayList<>(columnSize);
        isTexts = new ArrayList<>(columnSize);

        for (FieldMeta fieldMeta : fields) {
            // 处理列信息
            columns.add(fieldMeta.columnName);
            upColumns.add(CharUtils.toUpperCase(fieldMeta.columnName));
            isKey.add(fieldMeta.isKeyBool);
            mySQLType.add(fieldMeta.columnType);
            isUnsigned.add(fieldMeta.isUnsigned);
            isBinary.add(fieldMeta.isBinary);
            isTexts.add(fieldMeta.isText);
        }
    }


    public String getFullName() {
        return fullName;
    }

    public int getColumnSize() {
        return columnSize;
    }

    /**
     * 拷贝 成新的meta data 并且增加列信息
     *
     * @return
     */
    public TableMeta duplicateAndAddColumn(int size) {
        TableMeta newMeta = new TableMeta();
        newMeta.columnSize = size;
        newMeta.fullName = this.fullName;
        newMeta.columns = new ArrayList<>(size);
        newMeta.upColumns = new ArrayList<>(size);
        newMeta.isKey = new ArrayList<>(size);
        newMeta.mySQLType = new ArrayList<>(size);
        newMeta.isUnsigned = new ArrayList<>(size);
        newMeta.isBinary = new ArrayList<>(size);
        newMeta.isTexts = new ArrayList<>(size);

        newMeta.columns.addAll(this.columns);
        newMeta.upColumns.addAll(this.upColumns);
        newMeta.isKey.addAll(this.isKey);
        newMeta.mySQLType.addAll(this.mySQLType);
        newMeta.isUnsigned.addAll(this.isUnsigned);
        newMeta.isBinary.addAll(this.isBinary);
        newMeta.isTexts.addAll(this.isTexts);

        for (int i = this.columnSize; i < size; i ++) {
            newMeta.columns.add("unknown" + i);
            newMeta.upColumns.add("UNKNOWN" + i);
            newMeta.isKey.add(false);
            newMeta.mySQLType.add("varchar(255)");
            newMeta.isUnsigned.add(false);
            newMeta.isBinary.add(false);
            newMeta.isTexts.add(false);
        }

        return newMeta;
    }

    public static class FieldMeta {

        private String columnName;

        private String columnType;
        private boolean isUnsigned;
        private boolean isBinary;
        private boolean isText;

        private String isNullable;
        private boolean isNullableBool;

        private String isKey;
        private boolean isKeyBool;
        private String defaultValue;
        private String extra;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) { // 列名 统一小写
            this.columnName = CharUtils.toLowerCase(columnName);
        }

        public String getColumnType() {
            return columnType;
        }

        public boolean isBinary() {
            return isBinary;
        }

        public boolean isText() {
            return isText;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
            String upColumnType = CharUtils.toUpperCase(columnType);

            isUnsigned = StringUtils.contains(upColumnType, "UNSIGNED");
            isBinary = StringUtils.contains(upColumnType, "VARBINARY") ||
                    StringUtils.contains(upColumnType, "BINARY");
            isText = StringUtils.equals("LONGTEXT", upColumnType) ||
                    StringUtils.equals("MEDIUMTEXT", upColumnType) ||
                    StringUtils.equals("TEXT", upColumnType) ||
                    StringUtils.equals("TINYTEXT", upColumnType);

        }

        public String getIsNullable() {
            return isNullable;
        }

        public void setIsNullable(String isNullable) {
            this.isNullable = isNullable;
            String isNullableUpperCase = CharUtils.toUpperCase(isNullable);
            isNullableBool = StringUtils.equals(isNullableUpperCase, "YES");
        }

        public String getIsKey() {
            return isKey;
        }

        public void setIsKey(String isKey) {
            this.isKey = isKey;
            String isKeyUpperCase = CharUtils.toUpperCase(isKey);
            isKeyBool = StringUtils.equals(isKeyUpperCase, "PRI");
        }

        public String getDefaultValue() {
            return defaultValue;
        }

        public void setDefaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }

        public boolean isUnsigned() {
            return isUnsigned;
        }

        public boolean isKey() {
            return isKeyBool;
        }

        public boolean isNullable() {
            return isNullableBool;
        }

        public String toString() {
            return "FieldMeta [columnName=" + columnName + ", columnType=" + columnType + ", defaultValue="
                    + defaultValue + ", extra=" + extra + ", isNullable=" + isNullable + ", isKey=" + isKey + "]";
        }

    }
}
