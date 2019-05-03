package com.jd.binlog.zk;


import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.util.LogUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @func 循环删除异常关闭的zookeeper 链接
 *
 * @reason zookeeper client 自动重连
 *
 * @@author pengan
 *
 * @@apiNote
 */
public class ZkConnRecycler extends Thread {
    public static BlockingQueue<ILeaderSelector> conns = new LinkedBlockingQueue<>();

    public void run() {
        LogUtils.info.info("ZkConnRecycler service is running");
        while (true) {
            ILeaderSelector ls = null;

            try {
                if ((ls = conns.take()) != null) {
                    org.apache.zookeeper.ZooKeeper zk = ls.getClient().getZookeeperClient().getZooKeeper();
                    LogUtils.info.info("ZkConnRecycler begin close " + zk.getSessionId());
                    ls.close();
                    LogUtils.info.info("ZkConnRecycler close conn " + zk.getSessionId() + " success !");
                }
            } catch (Exception e) {
                try {
                    if (ls != null) {
                        org.apache.zookeeper.ZooKeeper zk = ls.getClient().getZookeeperClient().getZooKeeper();
                        LogUtils.info.info("ZkConnRecycler close conn " + zk.getSessionId() + " failure !");
                        conns.offer(ls);
                        LogUtils.info.info("conn " + zk.getSessionId() + "put into the queue again");
                    }
                } catch (Exception e1) {
                    LogUtils.error.error(e);
                }
            }
        }
    }
}
