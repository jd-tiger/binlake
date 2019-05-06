package com.jd.binlog.performance;

import com.jd.binlog.inter.perform.IPerformance;
import com.jd.binlog.util.TimeUtils;

/**
 * Created on 18-7-20
 *
 * @author pengan
 */
public class UMPPerformer implements IPerformance {

    @Override
    public void perform(String key, long when) {
        perform(key, when, TimeUtils.time());
    }

    @Override
    public void perform(String key, long when, long curr) {
        // TODO do something for the elapsed time
//        long elapse = curr - when * 1000;
//        CallerInfo callerInfo = Profiler.registerInfo(key, false, true);
//        try {
//            ReflectUtils.setFieldValue(callerInfo, "elapsedTime", elapse + "");
//        } catch (Throwable e) {
//            Profiler.functionError(callerInfo);
//        } finally {
//            Profiler.registerInfoEnd(callerInfo);
//        }
    }
}
