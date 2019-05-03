package com.jd.binlog.inter.zk;

import com.jd.binlog.inter.work.IBinlogWorker;
import com.jd.binlog.meta.Http;
import com.jd.binlog.meta.Meta;
import com.jd.binlog.meta.MetaInfo;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;

import java.util.Map;

/**
 * Created on 18-5-10
 * <p>
 * leader selector interface
 *
 * @author pengan
 */
public interface ILeaderSelector {
    /**
     * start leader selector
     */
    void startSelector();

    /**
     * 获取元数据信息
     *
     * @return
     */
    MetaInfo getMetaInfo();

    /**
     * 获取工作线程
     *
     * @return
     */
    IBinlogWorker getWork();

    /**
     * get client
     */
    CuratorFramework getClient();

    /**
     * get leader selector
     */
    LeaderSelector getLeaderSelector();

    /**
     * update znode state
     */
    void updateZNodesState(Meta.Terminal terminal, MetaInfo metaInfo) throws Exception;

    /**
     * update counter
     */
    void updateCounter(MetaInfo metaInfo) throws Exception;


    /**
     * 放弃leader 但是不退出ephemeral node connection
     */
    void abandonLeaderShip();

    /**
     * @ kill work thread and abandon leader
     */
    void killWorkAndAbandonLeader();

    /**
     * 关闭清理
     */
    void close();

    /**
     * 手动强制刷新binlog position
     */
    void refreshLogPos() throws Exception;

    /**
     * 获取最新的binlog offset
     */
    Meta.BinlogInfo getLatestBinlogPos();


    /**
     * refresh candidate and set leader
     * @param req:
     */
    void refreshCandidate(Http.RefreshRequest req);
}
