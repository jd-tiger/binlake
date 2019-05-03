package com.jd.binlog.work;

import com.jd.binlog.BinlogService;
import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IBinlogHandler;
import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.net.RuleTask;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created on 18-5-21
 * <p>
 * binlog 处理线程  中间携带buffer 作为缓冲区
 *
 * @author pengan
 */
public class BinlogHandler extends Thread implements IBinlogHandler {
    /**
     * 工作线程
     */
    private IBinlogWorker worker;

    /**
     * 是否是完全乱序
     */
    private boolean isMess;

    /**
     * 所有规则
     */
    private List<IRule> rules;

    /**
     * buffer queue
     */
    private LinkedBlockingQueue<IMessage> queue = new LinkedBlockingQueue<IMessage>();


    public BinlogHandler(List<IRule> rules, String name, boolean isMess, IBinlogWorker worker) {
        super(name + "-handler");
        this.rules = rules;
        this.worker = worker;
        this.isMess = isMess;
    }


    /**
     * 将消息 发往队列
     *
     * @param msg
     */
    public void offer(IMessage msg) {
        LinkedBlockingQueue<IMessage> queue = this.queue;
        if (queue != null) {
            queue.offer(msg);
        }
    }


    @Override
    public void run() {
        IMessage msg = null;
        // 获取消息 并且worker 未close 并且线程为中断
        try {
            while (!isInterrupted()) {
                final IBinlogWorker worker = this.worker;
                if (worker == null || worker.isClosed()) {
                    return; // break
                }

                LinkedBlockingQueue<IMessage> queue = this.queue;
                if (queue == null) {
                    return; // break
                }

                msg = queue.take();
                if (!isMess) { // 大部分是非乱序的
                    invoke(msg);
                    // 继续获取数据
                    continue;
                }

                final IMessage fm = msg;
                BinlogService.executor.execute(() -> invoke(fm));
            }
        } catch (InterruptedException e) {
            IBinlogWorker worker = this.worker;
            if (worker != null && !worker.isClosed()) {
                worker.close();
            }
        }
    }

    /**
     * 匹配 rule 规则
     *
     * @param msg
     */
    private void invoke(IMessage msg) {
        try {
            // assemble message to queue in parallel wait until all rules finished
            BinlogService.rFJP.invoke(new RuleTask(msg, rules));
        } finally {
            // 完全清空
            msg.clear();
        }
    }

    /**
     * 清空引用
     */
    public void clear() {
        this.worker = null;
        this.queue.clear();
        this.queue = null;
        this.rules = null;

        this.interrupt();
    }

    @Override
    public void startHandler() {
        start();
    }
}
