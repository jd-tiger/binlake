package com.jd.binlog.exception;

/***
 * operation Type for error code
 */
public enum OperationType {
    Retry(1000),  //  retry 重试
    Stop(1001);   //  stop no need to retry

    private final int id;

    OperationType(int i) {
        this.id = i;
    }

    /***
     * take the number into operation tp
     * @param i
     * @return
     */
    public static OperationType valueOf(int i) {
        switch (i) {
            case 1000:
                return Retry;
            case 1001:
                return Stop;
            default:
                return Retry;
        }
    }


}
