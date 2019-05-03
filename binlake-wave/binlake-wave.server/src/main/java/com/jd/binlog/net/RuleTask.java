package com.jd.binlog.net;

import com.jd.binlog.inter.msg.IMessage;
import com.jd.binlog.inter.rule.IRule;
import com.jd.binlog.util.LogUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created on 18-5-15
 *
 * @author pengan
 */
public class RuleTask extends RecursiveAction {
    IMessage msg;
    List<IRule> rules;

    /**
     * 分组策略
     *
     */
    ArrayList<Integer[]> strategy;

    public RuleTask(IMessage msg, List<IRule> rules) {
        this.msg = msg.duplicate(); // 复制所有数据
        this.rules = rules;
    }

    @Override
    protected void compute() {
        int size = rules.size();
        if (size == 1) {
            rules.get(0).convert(msg); // 只能get 不能 remove
            clear();
            return;
        }

        List<RuleTask> rts = new ArrayList<RuleTask>(size);
        for (int i = 0; i < size; i++) {
            List<IRule> rs = new ArrayList<IRule>(1);
            rs.add(rules.get(i));

            RuleTask rt = new RuleTask(msg, rs);
            rts.add(rt);

            rt.fork(); // fork
        }

        for (RuleTask rt : rts) {
            rt.join(); // join
        }
    }

    /**
     * 清空引用
     *
     */
    void clear() {
        rules = null;
        msg = null;
    }

    /**
     * 分组策略
     *
     * @return
     */
    private void groupStrategy() {
//        ArrayList<Integer[]> group = new ArrayList<Integer[]>();
//        if (ruleNum == 1) { // 只有一个分组
//            group.add(new Integer[]{0});
//            strategy = group;
//        }
//
//        if (ruleNum == 2) { // 只有倆个分组
//            group.add(new Integer[]{0});
//            group.add(new Integer[]{1});
//            strategy = group;
//        }
//
//        // 由于rules 不能随便更改 多线程影响
//        long pri = 0;
//        long[] pris = new long[ruleNum];
//        for (int i = 0; i < ruleNum; i++) {
//            pris[i] = rules.get(i).priority();
//            pri += pris[i];
//        }
//
//        long avg = pri / ruleNum;
//        int pre = -1;
//        for (int i = 0; i < ruleNum; i++) {
//            if (pris[i] > avg) {
//                group.add(new Integer[]{i}); // 单独一个享用一个线程
//            } else {
//                if (pre == -1) {
//                    pre = i; // 先存起来
//                    continue;
//                }
//                group.add(new Integer[]{pre, i}); // 带上下表 两个一组
//                pre = -1;
//            }
//        }
//
//        if (pre != -1) {
//            group.add(new Integer[]{pre});
//        }
//        strategy = group;
    }
}
