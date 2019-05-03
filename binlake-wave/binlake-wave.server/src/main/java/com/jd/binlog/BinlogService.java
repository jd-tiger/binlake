package com.jd.binlog;

import com.jd.binlog.config.ConfigLoader;
import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.config.bean.ServerConfig;
import com.jd.binlog.domain.GTIDTracker;
import com.jd.binlog.domain.TimeTracker;
import com.jd.binlog.http.HttpService;
import com.jd.binlog.inter.alarm.IAlarm;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.inter.work.IWorkInitializer;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.inter.zk.IZkClient;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.net.BufferPool;
import com.jd.binlog.net.ProduceTask;
import com.jd.binlog.util.ExecutorUtils;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.work.BinlogWorker;
import com.jd.binlog.zk.ZkClient;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;


/**
 * Created by pengan on 16-12-15.
 */
public class BinlogService {
    private static BinlogService INSTANCE = new BinlogService();

    public static ConcurrentHashMap<String, ILeaderSelector> lsm; // 必须是线程安全

    /**
     * rule fork join pool
     */
    public static ForkJoinPool rFJP;

    /**
     * executor service
     */
    public static ThreadPoolExecutor executor;

    public static ArrayList<LinkedBlockingQueue<IRule.RetValue>> queues;

    private IZkClient zkClient;
    private HttpService http;
    private ConfigLoader loader;
    private String host;

    public void start() throws Exception {
        LogUtils.info.info("start");
        lsm = new ConcurrentHashMap<>();

        loader = new ConfigLoader();
        loader.load();

        final ServerConfig serverConf = loader.getServerConf();
        this.host = loader.getServerConf().getHost();
        final HttpConfig httpConf = loader.getHttpConfig();

        final BufferPool bufferPool = new BufferPool(32, 1024);

        // 设置 重试报警次数 从倒数第二次就开始报警
        IAlarm.retryTimes.set(serverConf.getDumpLatch() - 2);

        // take processor number
        int processor = serverConf.getProcessors();
        executor = ExecutorUtils.create("binlog-service-executor", processor);

        // throttle buffer size
        final int throttleSize = serverConf.getThrottleSize();

        // concurrent queue only exist in one dump host
        queues = new ArrayList<LinkedBlockingQueue<IRule.RetValue>>(processor);
        ProduceTask[] tasks = new ProduceTask[processor];
        for (int i = 0; i < processor; i++) {
            LinkedBlockingQueue<IRule.RetValue> queue = new LinkedBlockingQueue<IRule.RetValue>();
            queues.add(queue);
            tasks[i] = new ProduceTask(queue, i);
        }

        final int fp = processor;

        IWorkInitializer wi = (metaInfo, preLeader, selector) -> {
            // 策略 先根据gtid ===> 异常 ==> 时间戳
            if (metaInfo.getBinlogInfo().getExecutedGtidSets().equals("") ||
                    !metaInfo.getBinlogInfo().getWithGTID()) {
                TimeTracker tracker = new TimeTracker(metaInfo, httpConf);
                tracker.binlogTrack();
            } else {
                try {
                    GTIDTracker tracker = new GTIDTracker(metaInfo, httpConf);
                    tracker.binlogTrack();
                } catch (Exception e) {
                    LogUtils.error.error("GTID tracker error:" + metaInfo.getHost(), e);
                    Meta.BinlogInfo binlogInfo = metaInfo.getBinlogInfo().setExecutedGtidSets("").setWithGTID(false);
                    metaInfo.setBinlogInfo(binlogInfo);
                    TimeTracker tracker = new TimeTracker(metaInfo, httpConf);
                    tracker.binlogTrack();
                }
            }

            if (LogUtils.debug.isDebugEnabled()) {
                LogUtils.debug.debug("MySQL host after:" + metaInfo.getInstanceIp());
            }

            metaInfo.setLeader(host + ":" + loader.getServerConf().getHttpPort());
            metaInfo.setPreLeader(preLeader);
            metaInfo.refreshBinlogInfo();// refresh binlog info

            BinlogWorker work = new BinlogWorker(fp, throttleSize, bufferPool, metaInfo, selector, queues);
            work.startWorking();
            return work;
        };

        rFJP = new ForkJoinPool(processor);

        LogUtils.info.info("init zk client");
        zkClient = new ZkClient(loader.getZkConf(), loader.getServerConf(), httpConf, wi, lsm);
        zkClient.start();

        LogUtils.info.info("start rpc service");
        http = new HttpService(loader.getServerConf().getHttpPort(), lsm, zkClient);

        // 启动queue 个线程
        for (ProduceTask task : tasks) {
            task.start();
        }
    }

    public static BinlogService getINSTANCE() {
        return INSTANCE;
    }
}
