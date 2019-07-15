# Binlake-Wave.Alarm

报警模块, 用来调用短信接口报警, 确保消息能够正常发送, 主要正对重试次数报警和切换报警 

## 功能  

* 实现接口 IAlarm
    ```sbtshell
    <a href="com.jd.binlog.inter.alarm.IAlarm">
    ```
* 报警分类  
    * 重试次数报警
        ```sbtshell
        AlarmUtils.mail(long times, String msg)    
        ```
    * 短信报警方式  
        ```text
        AlarmUtils.phone(long times, String msg)
        ```
        
## 实现方式  
* url post请求   
    采用公司统一的报警接口发送报警短信或者邮件 

* 报警格式   
    由于每个公司的报警内容以及报警的方式或者报警的邮件参数不一样, 所以采用统一的map形式 完成输出的输出 
    
    * JDMailParas   
        是 JD.com 邮件报警的方式, 专用于邮件的报警 
    
    * JDPhoneParas  
        是 JD.com 短信报警的方式, 专用于短信以及系统异常的报警   
