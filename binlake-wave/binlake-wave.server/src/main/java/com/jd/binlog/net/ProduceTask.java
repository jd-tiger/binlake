package com.jd.binlog.net;

import com.jd.binlog.inter.produce.IProducer;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.performance.PerformanceUtils;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.util.RetryUtils;
import com.jd.binlog.util.TimeUtils;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created on 18-5-17
 *
 * @author pengan
 */
public class ProduceTask extends Thread {
    LinkedBlockingQueue<IRule.RetValue> queue;

    public ProduceTask(LinkedBlockingQueue<IRule.RetValue> queue, int index) {
        super("producer " + index);
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            IRule.RetValue rv;
            while ((rv = queue.take()) != null) {
                long start = TimeUtils.time();
                try {
                    if (rv.worker.isClosed()) {
                        continue;
                    }

                    if (LogUtils.debug.isDebugEnabled()) {
                        LogUtils.debug.debug("key=" + new String(rv.key) + ",partition=" + rv.partition +
                                ",pos=" + rv.logPos.getFileName() + ":" + rv.logPos.getPosition());
                    }

                    final IProducer fp = rv.producer;
                    final IRule.RetValue frv = rv;

                    // 生产出现异常需要重试  并不能够保证每次都能够发送成功
                    // 发送端重试3次 如果返回
                    RetryUtils.retry(() -> {
                        fp.produce(frv);
                        return true;
                    }, 3); // 由于一般都有2s的超时 所以重试次数设置成 3

                    if (!rv.logPos.isCommit()) { // 非commit 事件
                        rv.worker.removeLogPosition(rv.logPos);
                    }
                } catch (Exception e) {
                    LogUtils.error.error("producer error ", e);

                    // 生产出现异常 当做io异常处理 增加重试次数
                    rv.worker.handleIOException(new IOException(e.getLocalizedMessage()));
                } finally {
                    PerformanceUtils.perform(PerformanceUtils.SEND_DELAY_KEY, rv.logPos.getWhen());
                    PerformanceUtils.perform(PerformanceUtils.PRODUCE_DELAY_KEY, start, TimeUtils.time());
                    rv.clear();
                }
            }
        } catch (InterruptedException e) {
            // 如果有异常 直接全部清空
            queue.clear();
        }
    }
}
