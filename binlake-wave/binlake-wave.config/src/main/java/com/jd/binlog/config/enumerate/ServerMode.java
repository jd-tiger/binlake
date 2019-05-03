package com.jd.binlog.config.enumerate;

import org.apache.commons.lang.StringUtils;

/**
 * Created by pengan on 17-3-21.
 *
 * wave 服务模式： 本地服务， 线上服务
 *
 * 默认采用在线服务 加上权限验证
 */
public enum ServerMode {
    LOCAL(0),
    ONLINE(1);

    private int type;
    private String name;

    ServerMode(int type) {
        this.type = type;
        switch (type) {
            case 0:
                this.name = "LOCAL";
                break;
            case 1:
                this.name = "ONLINE";
                break;
            case 2:
                this.name = "OTHER";
                break;
        }
    }

    public String getName() {
        return name;
    }

    public static ServerMode mode(String name) {
        if (!StringUtils.equalsIgnoreCase("ONLINE", name)) {
            return LOCAL;
        }
        return ONLINE;
    }
}
