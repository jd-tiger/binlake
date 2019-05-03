package com.jd.binlog.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by pengan on 17-1-4.
 */
public class ExecutorUtils {

    public static final ThreadPoolExecutor create(String name, int size) {
        return create(name, size, true);
    }

    public static final ThreadPoolExecutor create(String name, int size, boolean isDaemon) {
        ThreadFactoryUtil factory = new ThreadFactoryUtil(name, isDaemon);
        return new ThreadPoolExecutor(size, size, Long.MAX_VALUE, TimeUnit.NANOSECONDS,
                new LinkedBlockingQueue<Runnable>(), factory);
    }

    private static class ThreadFactoryUtil implements ThreadFactory {
        private final ThreadGroup group;
        private final String namePrefix;
        private final AtomicInteger threadId;
        private final boolean isDaemon;

        public ThreadFactoryUtil(String name, boolean isDaemon) {
            SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = name;
            this.threadId = new AtomicInteger(0);
            this.isDaemon = isDaemon;
        }

        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadId.getAndIncrement());
            t.setDaemon(isDaemon);
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }

}
