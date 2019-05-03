package com.jd.binlog.inter.zk;

import com.jd.binlog.meta.Http;

/**
 * Created on 18-5-11
 * <p>
 * zookeeper client interface
 *
 * @author pengan
 */
public interface IZkClient {
    /**
     * add leader selector according to zk client
     *
     * @param req
     * @throws Exception
     */
    void addLeaderSelector(Http.RefreshRequest req) throws Exception;

    /**
     * start zk client
     *
     * @throws Exception
     */
    void start() throws Exception;
}
