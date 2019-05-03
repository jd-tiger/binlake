package com.jd.binlog.util;

import java.util.concurrent.Callable;

/**
 * Created on 18-7-16
 *
 * @author pengan
 */
public class RetryUtils {
    /**
     * 重试函数
     *
     * @param call  返回true 表示成功
     * @param times 重试次数
     */
    public static void retry(Callable<Boolean> call, int times) throws Exception {
        int count = 1;
        boolean ret = false; // 返回结果正常为true
        Exception exp = null; // 防止异常丢失 保留最开始异常
        while (!ret) {
            count++; // 是否执行 都将 重试次数 + 1 避免无线循环
            try {
                ret = call.call();
            } catch (Exception e) {
                exp = e;
                if (count > times) { // 异常情况下抛出异常
                    throw e;
                }
            }
            if (count > times) { // 非异常 正常返回false 但是次数已经超出
                if (exp != null) {
                    throw exp;
                }
                throw new Exception("retry exceed over " + times);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Callable<Boolean> call = new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                System.err.println("return success");
                return true;
            }
        };

        retry(call, 2);
    }
}
