package com.jd.binlog.inter.work;

import com.jd.binlog.inter.msg.IMessage;

/**
 * Created on 18-5-22
 *
 * @author pengan
 */
public interface IBinlogHandler {
    /**
     * 往 handler 队列推送消息
     *
     * @param msg
     */
    void offer(IMessage msg);

    /**
     * 清空handler 对象 以及引用对象
     *
     */
    void clear();

    /**
     * 启动 handler
     *
     */
    void startHandler();
}
