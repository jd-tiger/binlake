package com.jd.binlog.inter.filter;

import com.jd.binlog.meta.Meta;

import java.util.List;
import java.util.Set;

/**
 * Created on 18-5-14
 *
 * @return true 表示规则匹配 false 不匹配
 * @author pengan
 */
public interface IFilter {
    /**
     * 过滤事件类型
     *
     * @param eventType
     * @return
     */
    boolean filterEventType(int eventType);


    /**
     * 过滤表
     *
     * @param table
     * @return
     */
    boolean filterTable(String table);

    /**
     * 过滤列
     *
     * @param col
     * @return
     */
    boolean filterColumn(String col);

    /**
     * 是否有过滤规则
     *
     */
    boolean haveColumnFilter();

    /**
     * 获取伪劣信息
     *
     * @return
     */
    List<Meta.Pair> getFakeCols();

    /**
     * 获取业务主键信息
     *
     * @return
     */
    Set<String> getHashKeys();


    /**
     * 是否存在有业务主键
     */
    boolean haveHashKeys();
}
