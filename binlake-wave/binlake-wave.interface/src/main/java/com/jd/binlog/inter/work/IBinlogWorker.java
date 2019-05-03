package com.jd.binlog.inter.work;

import com.jd.binlog.dbsync.LogPosition;

import java.io.IOException;
import java.util.Map;

/**
 * work interface
 * <p>
 * Created by pengan on 17-5-2.
 */
public interface IBinlogWorker {
    int PACKET_HEAD_SIZE = 4;

    int MAX_PACKET_SIZE = 16 * 1024 * 1024;

    int THROTTLE_QUEUE_SIZE = 64;

    Object object = new Object();

    /**
     * startWorking
     */
    void startWorking(); // startWorking

    /**
     * close worker and handler and all
     *
     * @return
     */
    boolean close(); // close work

    /**
     * 是否已经关闭
     *
     * @return
     */
    boolean isClosed();

    /**
     * 获取最新的binlog 位置 并删除已经过期的节点
     * <p>
     * 只会找到最新的commit位置push 到zk
     * <p>
     * ----------------------|    |--------------------|    |--------------------|
     * node1.isCommit = true | -> | node2.isCommit=true| -> |node3.isCommit=false| ...
     * ----------------------|    |--------------------|    |--------------------|
     * <p>
     * 返回 node2
     *
     * @return
     */
    LogPosition getLatestLogPosWithRm();

    /**
     * 只是读取最新的binlog offset 原理同
     *
     * @func: getLatestLogPosWithRm
     * <p>
     * 但是不删除log-position
     */
    LogPosition getLatestLogPos();

    /**
     * 处理异常：
     * <p>
     * 所有异常： 仅仅是退出leader 不关闭leaderSelector 因为dump会重试
     *
     * @param exp
     */
    void handleIOException(IOException exp); // handle io exception

    /**
     * 处理 error 异常
     *
     * @param exp
     */
    void handleError(Throwable exp); // handle any other error

    /**
     * 删除 binlog position in queue
     *
     * @param target
     */
    void removeLogPosition(LogPosition target);

    /**
     * keep dump data on worker thread
     */
    void keepDump();

    /**
     * monitor including: delay and start time and etc any what you want
     */
    Map<String, Object> monitor();
}
