package com.jd.binlog.exception;

/**
 * Created by pengan on 16-12-18.
 */
public enum ErrorCode {

    ERR_TABLE_META(1219), // 获取元数据信息失败
    ERR_BINLOG_FILE_NOT_EXIST(1220),
    ERR_MESSAGE_SEND(1230),
    ERR_NOT_FIND_MINMAX_INDEX(1231),

    CONNECTION_FAILED(10001),
    PARSER_ERROR(10002),
    NOT_SUPPORT_EVENT(10003),
    ERR_BINLOG_FORMAT(10004),

    ERR_MQ_WRITE(10006),
    ERR_ZK_WRITE(10007),
    ERR_DOMAIN_SWITCH_TRACKER(10010),
    ERR_DOMAIN_PARSE(10011),

    ERR_INIT_MQ_PRODUCER(10012),
    ERR_COLUMN_NUM(10013),
    ERR_PRODUCER_CLASS_NOT_FOUND(10014), // producer 类名未找到
    ERR_PRODUCER_CONSTRUCTOR(10015), // producer 构造方法 不符合规格
    ERR_PRODUCER_ERR_INIT(10016), // producer 初始化失败 很有可能是参数错误
    ERR_PRODUCER_MSG_SEND(10017), // producer 初始化失败 很有可能是参数错误

    ERR_CONVERTER_CLASS_NOT_FOUND(10018), // converter 类名未找到
    ERR_CONVERTER_CONSTRUCTOR(10019), // converter 构造函数初始化失败
    ERR_CONVERTER_INIT(10019), // converter 初始化失败

    ERR_RULE_NUM(10020),

    ERR_UNKNOWN(20000);

    private final int errorCode;

    ErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    @Override
    public String toString() {
        return errorCode + "-" + name();
    }

    public int getErrorCode() {
        return errorCode;
    }
}

