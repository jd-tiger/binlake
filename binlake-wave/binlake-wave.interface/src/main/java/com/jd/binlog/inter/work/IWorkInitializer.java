package com.jd.binlog.inter.work;

import com.jd.binlog.inter.zk.ILeaderSelector;
import com.jd.binlog.meta.MetaInfo;

/**
 * Created on 18-5-11
 * <p>
 * 初始化 work 接口
 *
 * @author pengan
 */
public interface IWorkInitializer {
    /**
     * 初始化 work
     * <p>
     *     域名切换检查
     * </p>
     * <p>
     *     刷新leader
     * </p>
     * <p>
     *     启动worker
     * </p>
     *
     * @param metaInfo
     * @param preLeader
     * @param selector
     *
     * @return
     * @throws Exception
     */
    IBinlogWorker initWork(MetaInfo metaInfo, String preLeader, ILeaderSelector selector) throws Exception;
}
