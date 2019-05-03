package com.jd.binlog.config;

import com.jd.binlog.config.bean.HttpConfig;
import com.jd.binlog.config.bean.ServerConfig;
import com.jd.binlog.config.bean.ZKConfig;
import com.jd.binlog.util.LogUtils;
import com.jd.binlog.util.NetUtils;
import com.jd.binlog.util.ReflectUtils;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Created by pengan on 16-12-27.
 *
 * This Loader is the entrance for configuration
 *
 * if want to refresh all configuration then just reload again is ok
 */
public class ConfigLoader {
    private ServerConfig serverConf;
    private ZKConfig zkConf;
    private HttpConfig httpConfig;

    public ConfigLoader() {
    }

    public void load() throws Exception {
        LogUtils.info.info("load");
        InputStream inputStream = ConfigLoader.class.getResourceAsStream("/config.properties");
        Properties props = new Properties();
        props.load(inputStream);

        String value = null;
        String field = null;
        this.zkConf = new ZKConfig();
        for (Map.Entry<String, String> entry : Constants.ZK_FIELDS.entrySet()) {
            value = props.getProperty(entry.getKey(), Constants.ZK_FIELDS_DEFAULT_VALUE.get(entry.getKey()));
            field = entry.getValue();
            ReflectUtils.setFieldValue(zkConf, field, value);
        }

        this.serverConf = new ServerConfig();
        for (Map.Entry<String, String> entry : Constants.SERVER_FIELDS.entrySet()) {
            value = props.getProperty(entry.getKey(), Constants.SERVER_FIELDS_DEFAULT_VALUE.get(entry.getKey()));
            while (value.endsWith("/")) {
                value = value.substring(0, value.lastIndexOf("/"));
            }
            field = entry.getValue();
            ReflectUtils.setFieldValue(serverConf, field, value);
        }

        ReflectUtils.setFieldValue(this.serverConf, "host", NetUtils.getHostAddress());

        this.httpConfig = new HttpConfig();
        validate();
    }

    private void validate() {
        if (this.serverConf != null) {
            serverConf.validate();
        }

        if (this.zkConf != null) {
            zkConf.validate();
        }
    }

    public ServerConfig getServerConf() {
        return serverConf;
    }

    public ZKConfig getZkConf() {
        return zkConf;
    }

    public HttpConfig getHttpConfig() {
        return httpConfig;
    }
}
