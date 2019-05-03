package com.jd.binlog.filter.aviater;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.jd.binlog.exception.BinlogException;
import com.jd.binlog.inter.filter.IEventFilter;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * 基于aviater进行tableName简单过滤计算，不支持正则匹配
 * 
 * @author jianghang 2012-7-20 下午05:53:30
 */
public class AviaterSimpleFilter implements IEventFilter<String> {

    private static final String SPLIT             = ",";

    private static final String FILTER_EXPRESSION = "include(list,target)";

    private final Expression exp               = AviatorEvaluator.compile(FILTER_EXPRESSION, true);

    private final List<String> list;

    public AviaterSimpleFilter(String filterExpression){
        if (StringUtils.isEmpty(filterExpression)) {
            list = new ArrayList<String>();
        } else {
            String[] ss = filterExpression.toLowerCase().split(SPLIT);
            list = Arrays.asList(ss);
        }
    }

    public boolean filter(String filtered) throws BinlogException {
        if (list.isEmpty()) {
            return true;
        }
        if (StringUtils.isEmpty(filtered)) {
            return true;
        }
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("list", list);
        env.put("target", filtered.toLowerCase());
        return (Boolean) exp.execute(env);
    }

}
