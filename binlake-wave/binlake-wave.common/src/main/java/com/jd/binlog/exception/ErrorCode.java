package com.jd.binlog.exception;

import java.util.Locale;

/**
 * Created by pengan on 16-12-18.
 * 异常分为 三类
 * 1) MySQL 异常 与MySQL 发送异常
 * 2) ZK 异常 客户端连接异常
 * 3) MQ 异常 作为发送者发送异常
 * <p>
 * 结果分为两种:
 * 1) 重试 retry
 * 2) 停止 stop
 *
 * @author pengan
 */
public enum ErrorCode {
    // MySQL 错误 报警类型 需要重试
    WARN_MySQL_HANDSHAKE(10002, "wave %s on MySQL %s handshake with %s error %s"), // 发送MySQL handshake package 异常 %s: user/****
    WARN_MySQL_SET(10003, "wave %s query on MySQL %s set sql %s error %s"), // 执行 set command 异常 %s: sql
    WARN_MySQL_DUMP(10004, "wave %s on MySQL %s send dump command %s error %s"), // 执行 MySQL dump command 异常 %s: dump类型
    WARN_MySQL_ROWEVENT_PARSE(10005, "wave %s on MySQL %s parse %s error %s"), // 解析MySQL sql 语句异常 %s: offset
    WARN_MySQL_DDL_PARSE(10005, "wave %s on MySQL %s parse ddl %s error %s"), // 解析MySQL sql 语句异常 %s: ddl
    WARN_DOMAIN_RESOLVE(10006, "wave %s resolve domain %s previous host %s error %s"), // 解析MySQL 域名地址失败 本地dns失效 %s: pre-host
    WARN_QUERY_NO_FILE(10007, "wave %s dump MySQL %s with binlog file %s not exits error %s"),   // 初始化过程当中binlog file not exist %s: binlog file
    WARN_PRODUCE_ERROR(10008, "wave %s dump MySQL %s on topic %s error %s"), // produce message error  %s 表示MQ地址 以及 topic
    WARN_ZK_DELETE_PATH(10009, "wave %s dump MySQL %s delete path %s error %s"), // %s : delete path
    WARN_ZK_UPDATE_PATH(10010, "wave %s dump MySQL %s update path %s error %s"), // %s: update path
    WARN_ZK_START_NEWHOST(10011, "wave %s start new MySQL %s error"), // %s: mysql

    // 系统错误 直接返回报错 不需要重试
    ERR_PRODUCER_CLASS_NOT_FOUND(20001, "wave %s dump MySQL %s on produce class %s not found"), // producer 类名未找到 %s: producer class
    ERR_PRODUCER_CONSTRUCTOR(20002, "wave %s dump MySQL %s produce class constructor %s init error"), // producer 构造方法 不符合规格 %s: constructor type
    ERR_PRODUCER_ERR_INIT(20003, "wave %s dump MySQL %s producer %s init error"), // producer 初始化失败 很有可能是参数错误 %s: producer init error
    ERR_CONVERTER_CLASS_NOT_FOUND(20004, "wave %s dump MySQL %s  convert class %s not found"), // converter 类名未找到 %s: convert class
    ERR_CONVERTER_CONSTRUCTOR(20005, "wave %s dump MySQL %s convert class constructor %s not found"), // converter 构造函数初始化失败 %s: convert constructor type
    ERR_CONVERTER_INIT(20006, "wave %s dump MySQL %s convert class %s init error %s"), // converter 初始化失败  %s: convert class
    ERR_UNSUPPORT_ENCODE(20007, "wave %s dump MySQL %s encode charset %s error %s"), // unsupport encode %s: charset|offset:

    // 业务方使用错误 给出错误原因 不需要重试 重试也会继续报错
    ERR_RULE_EMPTY(30001, "wave %s dump MySQL %s empty rule error"),   // empty rule error
    ERR_DUMP_NO_TABLE(40001, "wave %s dump MySQL %s without TableMapEvent for table %s error %s"), // 获取 对应的table map log event失败
    ERR_DUMP_OFFSET(40002, "wave %s dump MySQL %s offset %s error %s"), // dump offset error %s: log position
    ERR_DUMP_BYTE_DECODE(40003, "wave %s dump MySQL %s decode on %s error %s"), // decode MySQL error %s: log position
    ERR_GTID_COMPARE(40004, "wave %s dump MySQL %s compare gtid %s error %s"), // compare gtid error %s: src gtid <=> dest gtid

    ERR_UNKNOWN(0, "wave %s dump MySQL %s error %s");  // 未知错误 需要重试


    public final int errorCode;
    public final String temp;

    ErrorCode(int errorCode, String temp) {
        this.errorCode = errorCode;
        this.temp = temp;
    }

    @Override
    public String toString() {
        return errorCode + "-" + name();
    }

    public static ErrorCode valueOfMySQLErrno(int errno) {
        switch (errno) {
            case 1236:
                /**
                 * mysql-5.7.25/include/mysqld_ername.h:239
                 *  239 { "ER_MASTER_FATAL_ERROR_READING_BINLOG", 1236, "Got fatal error %d from master when reading data from binary log: \'%-.320s\'" },
                 */
                return ERR_DUMP_OFFSET;
            default:
                return ERR_UNKNOWN;
        }
    }

    /**
     * according error number choose operation type
     *
     * @return
     */
    public OperationType according() {
        if (this.errorCode < ERR_PRODUCER_CLASS_NOT_FOUND.errorCode) {
            return OperationType.Retry;
        }

        return OperationType.Stop;
    }

    /**
     * assemble string error message
     *
     * @param paras
     * @return
     */
    public byte[] assemble(String... paras) {
        return String.format(temp, paras).getBytes();
    }
}

