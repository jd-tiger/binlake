package com.jd.binlog.exception;

/**
 * Created by pengan on 16-12-18.
 */
public class BinlogException extends RuntimeException {

    private final ErrorCode errorCode;
    private String paras;
    private Throwable exp;

    public BinlogException(ErrorCode errorCode, Throwable exp, String paras) {
        this.errorCode = errorCode;
        this.paras = paras;
        this.exp = exp;
    }

    public BinlogException(ErrorCode errorCode, String message) {
        this(errorCode, message, null);
    }

    public BinlogException(ErrorCode errorCode, Throwable throwable) {
        this(errorCode, "", throwable);
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

    /**
     * call message must make sure that paras not null
     *
     * @param pre
     * @return
     */
    public byte[] message(String... pre) {
        int len = pre.length;
        if (paras != null) {
            len += 1;
        }

        if (exp != null) {
            len += 1;
        }

        String[] ps = new String[len];

        int i = 0;
        for (String s : pre) {
            ps[i++] = s;
        }

        if (paras != null) {
            ps[i++] = paras;
        }

        if (exp != null) {
            ps[i] = exp.getLocalizedMessage();
        }

        return String.format(errorCode.temp, ps).getBytes();
    }
}
