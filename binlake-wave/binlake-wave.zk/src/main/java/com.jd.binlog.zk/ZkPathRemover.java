package com.jd.binlog.zk;

import com.jd.binlog.meta.Meta;
import com.jd.binlog.util.GzipUtil;
import com.jd.binlog.util.LogUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created on 18-7-17
 * <p>
 * 由于 删除MySQL 节点 会引发中断导致当前线程退出
 * <p>
 * 所以需要完成切换到 回收线程
 *
 * @author pengan
 */
public class ZkPathRemover extends Thread {
    private static final String LOCK_NAME = "lock-";

    public static LinkedBlockingQueue<String> paths = new LinkedBlockingQueue<>();
    final String zk;

    public ZkPathRemover(String zk) {
        super("zk-path-remover");
        this.zk = zk;
    }

    @Override
    public void run() {
        CuratorFramework client = CuratorFrameworkFactory.newClient(zk,
                new RetryNTimes(3, 6000));
        client.start();
        while (true) {
            String instance = null;
            try {
                if ((instance = paths.take()) != null) {
                    LogUtils.info.info("remove path " + instance);
                    if (client.checkExists().forPath(instance) == null) {
                        // 路径不存在
                        continue;
                    }

                    byte[] bts = null;
                    try {
                        bts = client.getData().forPath(instance);
                    } catch (Throwable exp) {
                        // client get data from zk server error so close zk then create a new connection
                        client = newClient(client, instance);
                        continue;
                    }

                    // old db info
                    Meta.DbInfo odb = Meta.DbInfo.unmarshalJson(bts);
                    LogUtils.info.info("old db info " + odb);

                    switch (odb.getState()) {
                        case ONLINE: { // 设置 offline
                            odb.setState(Meta.NodeState.OFFLINE);

                            try {
                                client.setData().forPath(instance, Meta.DbInfo.marshalJson(odb));
                            } catch (Throwable exp) {
                                // client get data from zk server error so close zk then create a new connection
                                client = newClient(client, instance);
                                continue;
                            }
                            break;
                        }
                        default:
                            break;
                    }

                    try {
                        // 检查临时节点
                        client.delete().deletingChildrenIfNeeded().forPath(instance);
                    } catch (Throwable exp) {
                        // client get data from zk server error so close zk then create a new connection
                        client = newClient(client, instance);
                    }
                }
            } catch (Throwable e) {
                // 当前线程一旦启动 则永远不退出
            }
        }
    }

    /**
     * close old client then offer path to paths
     *
     * @param oc
     * @param path
     * @return
     */
    private CuratorFramework newClient(CuratorFramework oc, String path) {
        CloseableUtils.closeQuietly(oc);
        CuratorFramework client = CuratorFrameworkFactory.newClient(zk,
                new RetryNTimes(3, 6000));
        client.start();
        paths.offer(path);
        return client;
    }

    public static void main(String[] args) throws InterruptedException {
        ZkPathRemover rm = new ZkPathRemover("127.0.0.1:2181");
        rm.start();

        paths.offer("/zk/wave3/172.22.163.107:3359");
        Thread.sleep(10000);
        System.err.println("remove path success");
    }
}
