package com.jd.binlog;

import com.jd.binlog.inter.alarm.IAlarm;
import com.jd.binlog.inter.perform.IPerformance;
import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.util.LogUtils;
import org.apache.log4j.helpers.FileWatchdog;
import org.apache.log4j.helpers.LogLog;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Created by pengan on 17-3-13.
 * <p>
 * 实时参数写入器初始化
 */
public class RealTimeInitializer {
    // 强制调用 binlog offset的刷新 主要要断开 jvm 的时候开启参数
    public static volatile boolean FLUSH_BINLOG_OFFSET = false;
    private static final String format = "yyyy-MM-dd HH:mm:ss";

    public static void configureAndWatch(long delay) {
        String home = System.getProperty("wave.home");

        if (home == null) {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            LogLog.warn(sdf.format(new Date()) + " [wave.home] is not set.");
        } else {
            ConfigWatchDog watchDog = new ConfigWatchDog(home + File.separator + "/conf/realtime.properties");
            watchDog.setName("ConfigWatchDog");
            watchDog.setDelay(delay);
            watchDog.start();
        }
    }

    private static final class ConfigWatchDog extends FileWatchdog {

        public ConfigWatchDog(String filename) {
            super(filename);
        }

        @Override
        public void doOnChange() {
            try {
                realTime(filename);
                LogUtils.info.info("load special config success");
            } catch (IOException e) {
                LogUtils.error.error("load special config error", e);
            }
        }
    }

    /**
     * 使用逗号分隔符，严格区分大小写
     *
     * @throws IOException
     */
    private static void realTime(String special) throws IOException {
        InputStream inputStream = new FileInputStream(special);
        Properties props = new Properties();
        try {
            props.load(inputStream);

            String phones = props.getProperty("alarm.contact.phone.number", "18515819096,15726816160");
            String token = props.getProperty("alarm.contact.token", "J1itu2OlRF2ThgWYK8yPcQ==");
            String alarmUrl = props.getProperty("alarm.contact.url", "http://api.dbs.jd.com:9000/godbs/sendText/");

            LogUtils.info.info("phones:" + phones + ", token:" + token + ", url:" + alarmUrl);

            IAlarm.phones.clear();
            for (String phone : phones.split(",")) {
                if (phone.trim().length() != 0) {
                    IAlarm.phones.add(phone.trim());
                }
            }

            if (token.trim().length() != 0) {
                IAlarm.token.set(token.trim());
            }

            if (alarmUrl.trim().length() != 0) {
                IAlarm.url.set(alarmUrl.trim());
            }

            /**
             * 强制刷新 binlog 日志
             */
            FLUSH_BINLOG_OFFSET = Boolean.parseBoolean(
                    props.getProperty("flush.binlog.offset", "false"));

            LogUtils.info.info("sync.binlog.offset : " + FLUSH_BINLOG_OFFSET);
            if (FLUSH_BINLOG_OFFSET) {
                for (Map.Entry<String, ILeaderSelector> entry : BinlogService.lsm.entrySet()) {
                    try {
                        entry.getValue().refreshLogPos();
                    } catch (Throwable exp) {
                        LogUtils.error.error(entry.getKey() + " stop jmv refresh binlog offset to zk error", exp);
                    }
                }
            }

            /**
             * 是否介入性能检测 监控
             */
            IPerformance.PERFORM_ACCESS.set(
                    Boolean.parseBoolean(props.getProperty("ump.access", "false")));

            // 这里将会作为警告 能够判断是否已经完成binlog 位置的flush
            LogUtils.info.info("load real time properties ok");
        } finally {
            inputStream.close();
        }
    }
}
