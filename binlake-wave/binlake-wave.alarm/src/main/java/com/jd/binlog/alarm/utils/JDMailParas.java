package com.jd.binlog.alarm.utils;

import com.jd.binlog.alarm.AlarmUtils;

import java.util.LinkedHashMap;
import java.util.Map;

/***
 * JDMailParas mail http parameters
 *
 * @author pengan 2019年05月23日
 */
public class JDMailParas {
    /***
     *      * https://cf.jd.com/pages/viewpage.action?pageId=149821628
     *      * <p>
     *      * "ToAddress":"zhangzhanyue1@jd.com,tianguangyu@jd.com"
     *      * <p>
     *      * "CcAddress":"yfzhangshicong@jd.com"
     *      * <p>
     *      * "Subject":"插入标题"
     *      * <p>
     *      * "Content":"插入内容" (支持html格式邮件内容)
     *      * <p>
     *      * "Attachments":"附件在邮件服务器上绝对地址"
     *
     * @param to: to users
     * @param sub : subject
     * @param content: content for mail
     * @return
     */
    public static Map<String, Object> mailParas(String[] to, String sub, byte[] content) {
        Map<String, Object> paras = new LinkedHashMap<>();
        paras.put("ToAddress", String.join(",", to));
        paras.put("CcAddress", String.join(",", AlarmUtils.adminMails()));
        paras.put("Subject", sub);
        paras.put("Content", content);
        return paras;
    }
}
