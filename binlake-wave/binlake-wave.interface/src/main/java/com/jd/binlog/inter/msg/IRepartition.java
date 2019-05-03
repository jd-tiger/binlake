package com.jd.binlog.inter.msg;

import com.jd.binlog.inter.rule.IRule;

/**
 * Created on 18-5-16
 *
 * @author pengan
 */
public interface IRepartition {
    Object object = new Object();

    void offer(IRule.RetValue retValue);
}