package com.jd.binlog.parser;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.oro.text.regex.Perl5Matcher;

import com.jd.binlog.protocol.WaveEntry;
import com.jd.binlog.util.PatternUtils;

/**
 * Created by hp on 14-9-3.
 */
public class SimpleDdlParser {

    public static final String CREATE_PATTERN = "^\\s*CREATE\\s*(TEMPORARY)?\\s*TABLE\\s*(.*)$";
    public static final String DROP_PATTERN = "^\\s*DROP\\s*(TEMPORARY)?\\s*TABLE\\s*(.*)$";
    public static final String ALERT_PATTERN = "^\\s*ALTER\\s*(IGNORE)?\\s*TABLE\\s*(.*)$";
    public static final String TRUNCATE_PATTERN = "^\\s*TRUNCATE\\s*(TABLE)?\\s*(.*)$";
    public static final String TABLE_PATTERN = "^(IF\\s*NOT\\s*EXISTS\\s*)?(IF\\s*EXISTS\\s*)?(`?.+?`?[;\\(\\s]+?)?.*$"; // 采用非贪婪模式
    public static final String INSERT_PATTERN = "^\\s*(INSERT|MERGE|REPLACE)(.*)$";
    public static final String UPDATE_PATTERN = "^\\s*UPDATE(.*)$";
    public static final String DELETE_PATTERN = "^\\s*DELETE(.*)$";
    public static final String RENAME_PATTERN = "^\\s*RENAME\\s*TABLE\\s*(.*?)[\\s`]+TO[\\s`]+(.*?)$";


    // RENAME TABLE `b2b_trade100`.`b2b_order_main` TO `b2b_trade100`.`_b2b_order_main_old`, `b2b_trade100`.`_b2b_order_main_new` TO `b2b_trade100`.`b2b_order_main`
    /**
     * <pre>
     * CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX index_name
     *         [index_type]
     *         ON tbl_name (index_col_name,...)
     *         [algorithm_option | lock_option] ...
     *
     * http://dev.mysql.com/doc/refman/5.6/en/create-index.html
     * </pre>
     */
    public static final String CREATE_INDEX_PATTERN = "^\\s*CREATE\\s*.*?\\s*INDEX\\s*(.*?)\\s*ON\\s*(.*?)$";
    public static final String DROP_INDEX_PATTERN = "^\\s*DROP\\s*INDEX\\s*(.*?)\\s*ON\\s*(.*?)$";

    public static void main(String[] args) {
        String rename = "USER DB;RENAME TABLE `b2b_trade100`.`b2b_order_main` TO `b2b_trade100`.`_b2b_order_main_old`, `b2b_trade100`.`_b2b_order_main_new` TO `b2b_trade100`.`b2b_order_main`";
        rename = "GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'admin'@'%' identified by 'admin'";

        DdlResult rst = parse(rename, "b2b_trade100");

        System.err.println(rst);
        for (DdlResult rst1 : rst.rsts) {
            System.err.println(rst1);
        }

    }

    public static DdlResult parse(String queryString, String schmeaName) {
        queryString = removeComment(queryString); // 去除/* */的sql注释内容
        DdlResult result = parseDdl(queryString, schmeaName, ALERT_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.ALTER);
            return result;
        }

        result = parseDdl(queryString, schmeaName, CREATE_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.CREATE);
            return result;
        }

        result = parseDdl(queryString, schmeaName, DROP_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.ERASE);
            return result;
        }

        result = parseDdl(queryString, schmeaName, TRUNCATE_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.TRUNCATE);
            return result;
        }

        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(RENAME_PATTERN))) {
            String[] renameSqls = queryString.split(",");
            List<DdlResult> rsts = new LinkedList<DdlResult>();
            switch (renameSqls.length) {
                case 0:
                    break;
                case 1:
                    addRenameDDlResult(queryString, schmeaName, rsts);
                    if (rsts.size() != 0) {
                        rsts.get(0).rsts = rsts;
                        return rsts.get(0);
                    }

                    break;
                default:
                    addRenameDDlResult(renameSqls[0], schmeaName, rsts);

                    for (int index = 1; index < renameSqls.length; index++) {
                        addRenameDDlResult("RENAME TABLE " + renameSqls[index], schmeaName, rsts);
                    }
                    if (rsts.size() != 0) {
                        rsts.get(0).rsts = rsts;
                        return rsts.get(0);
                    }
                    break;
            }
        }

        result = parseDdl(queryString, schmeaName, CREATE_INDEX_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.CINDEX);
            return result;
        }

        result = parseDdl(queryString, schmeaName, DROP_INDEX_PATTERN, 2);
        if (result != null) {
            result.setType(WaveEntry.EventType.DINDEX);
            return result;
        }

        result = new DdlResult(schmeaName);
        if (isDml(queryString, INSERT_PATTERN)) {
            result.setType(WaveEntry.EventType.INSERT);
            return result;
        }

        if (isDml(queryString, UPDATE_PATTERN)) {
            result.setType(WaveEntry.EventType.UPDATE);
            return result;
        }

        if (isDml(queryString, DELETE_PATTERN)) {
            result.setType(WaveEntry.EventType.DELETE);
            return result;
        }

        result.setType(WaveEntry.EventType.QUERY);
        return result;
    }

    private static void addRenameDDlResult(String renameSql, String schmeaName, List<DdlResult> rsts) {
        DdlResult rst = null;
        if ((rst = parseRename(renameSql, schmeaName, RENAME_PATTERN)) != null) {
            rst.setType(WaveEntry.EventType.RENAME);
            rsts.add(rst);
        }
    }

    private static DdlResult parseDdl(String queryString, String schmeaName, String pattern, int index) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            DdlResult result = parseTableName(matcher.getMatch().group(index), schmeaName);
            return result != null ? result : new DdlResult(schmeaName); // 无法解析时，直接返回schmea，进行兼容处理
        }

        return null;
    }

    private static boolean isDml(String queryString, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            return true;
        } else {
            return false;
        }
    }

    private static DdlResult parseRename(String queryString, String schmeaName, String pattern) {
        Perl5Matcher matcher = new Perl5Matcher();
        if (matcher.matches(queryString, PatternUtils.getPattern(pattern))) {
            DdlResult orign = parseTableName(matcher.getMatch().group(1), schmeaName);
            DdlResult target = parseTableName(matcher.getMatch().group(2), schmeaName);
            if (orign != null && target != null) {
                return new DdlResult(target.getSchemaName(),
                        target.getTableName(),
                        orign.getSchemaName(),
                        orign.getTableName());
            }
        }

        return null;
    }

    private static DdlResult parseTableName(String matchString, String schmeaName) {
        Perl5Matcher tableMatcher = new Perl5Matcher();
        matchString = matchString + " ";
        if (tableMatcher.matches(matchString, PatternUtils.getPattern(TABLE_PATTERN))) {
            String tableString = tableMatcher.getMatch().group(3);

            tableString = StringUtils.removeEnd(tableString, ";");
            tableString = StringUtils.removeEnd(tableString, "(");
            tableString = StringUtils.trim(tableString);
            // 特殊处理引号`
            tableString = removeEscape(tableString);
            // 处理schema.table的写法
            String names[] = StringUtils.split(tableString, ".");
            if (names != null && names.length > 1) {
                return new DdlResult(removeEscape(names[0]), removeEscape(names[1]));
            } else {
                return new DdlResult(schmeaName, removeEscape(names[0]));
            }
        }

        return null;
    }

    private static String removeEscape(String str) {
        String result = StringUtils.removeEnd(str, "`");
        result = StringUtils.removeStart(result, "`");
        return result;
    }

    private static String removeComment(String sql) {
        if (sql == null) {
            return null;
        }

        String start = "/*";
        String end = "*/";
        while (true) {
            // 循环找到所有的注释
            int index0 = sql.indexOf(start);
            if (index0 == -1) {
                return sql;
            }
            int index1 = sql.indexOf(end, index0);
            if (index1 == -1) {
                return sql;
            }
            StringBuilder sb = new StringBuilder();
            sb.append(sql.substring(0, index0));
            sb.append(" ");
            sb.append(sql.substring(index1 + end.length()));
            sql = sb.toString();
        }
    }

    public static class DdlResult {

        private String schemaName;
        private String tableName;
        private String oriSchemaName; // rename ddl中的源表
        private String oriTableName; // rename ddl中的目标表
        private WaveEntry.EventType type;
        private List<DdlResult> rsts;

        public DdlResult() {
        }

        public DdlResult(String schemaName) {
            this.schemaName = schemaName;
        }

        public DdlResult(String schemaName, String tableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
        }

        public DdlResult(String schemaName, String tableName, String oriSchemaName, String oriTableName) {
            this.schemaName = schemaName;
            this.tableName = tableName;
            this.oriSchemaName = oriSchemaName;
            this.oriTableName = oriTableName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public WaveEntry.EventType getType() {
            return type;
        }

        public void setType(WaveEntry.EventType type) {
            this.type = type;
        }

        public String getOriSchemaName() {
            return oriSchemaName;
        }

        public void setOriSchemaName(String oriSchemaName) {
            this.oriSchemaName = oriSchemaName;
        }

        public String getOriTableName() {
            return oriTableName;
        }

        public void setOriTableName(String oriTableName) {
            this.oriTableName = oriTableName;
        }

        public List<DdlResult> getRsts() {
            return rsts;
        }

        @Override
        public String toString() {
            return "DdlResult [schemaName=" + schemaName + ", tableName=" + tableName + ", oriSchemaName="
                    + oriSchemaName + ", oriTableName=" + oriTableName + ", type=" + type + "]";
        }

    }

}
