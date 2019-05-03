package com.jd.binlake.tower.config;

import com.jd.binlog.util.ReflectUtils;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by pengan on 17-3-3.
 */
public class ConfigLoader {
    private static final Logger logger = Logger.getLogger(ConfigLoader.class);

    private TowerConfig config;

    public void load() throws Exception {
        logger.info("load");
        InputStream inputStream = ConfigLoader.class.getResourceAsStream("/config.properties");
        Properties props = new Properties();
        props.load(inputStream);
        TowerConfig config = new TowerConfig();

        ReflectUtils.setFieldValue(config, "port",
                props.getProperty("tower.sever.port",
                        "9096"));
        this.config = config;
    }

    public TowerConfig getConfig() {
        return config;
    }
}