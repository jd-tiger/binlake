package com.jd.binlog.alarm.utils;


import com.jd.binlog.alarm.AlarmUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JDPhoneParas jd http phone alarm parameters
 *
 * @author pengan 2019年05月23日
 */
public class JDPhoneParas {
    /**
     * only send message to administrator
     *
     * @return
     */
    public static Map<String, Object> phoneParas(byte[] text) {
        Map<String, Object> paras = new LinkedHashMap<>();
        paras.put("text", text);
        paras.put("mobile_nums", AlarmUtils.adminPhones());
        return paras;
    }
}
