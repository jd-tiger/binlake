package com.jd.binlog.exception;

/**
 * Created by pengan on 16-12-18.
 */
public class BinlogException extends RuntimeException {

    private final ErrorCode errorCode;

    public BinlogException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    public BinlogException(ErrorCode errorCode, Throwable throwable) {
        this(errorCode, null, throwable);
    }

    public BinlogException(ErrorCode errorCode, String message, Throwable throwable) {
        super(message, throwable);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    @Override
    public String getMessage() {
        StringBuilder msg = new StringBuilder();
        msg.append(getErrorCode());
        if (super.getCause() != null) {
            msg.append("|").append(super.getCause().getMessage());
        }
        if (super.getMessage() != null) {
            msg.append("|").append(super.getMessage());
        }
        return msg.toString();
    }
}
