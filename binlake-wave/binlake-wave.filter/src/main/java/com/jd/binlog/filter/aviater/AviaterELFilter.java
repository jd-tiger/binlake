package com.jd.binlog.filter.aviater;

import com.googlecode.aviator.AviatorEvaluator;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.inter.filter.IEventFilter;
import com.jd.binlog.protocol.WaveEntry;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于aviater el表达式的匹配过滤
 * 
 * @author jianghang 2012-7-23 上午10:46:32
 */
public class AviaterELFilter implements IEventFilter<WaveEntry.Entry> {

    public static final String ROOT_KEY = "entry";
    private String expression;

    public AviaterELFilter(String expression){
        this.expression = expression;
    }

    public boolean filter(WaveEntry.Entry entry) throws BinlogException {
        if (StringUtils.isEmpty(expression)) {
            return true;
        }

        Map<String, Object> env = new HashMap<String, Object>();
        env.put(ROOT_KEY, entry);
        return (Boolean) AviatorEvaluator.execute(expression, env);
    }

}
