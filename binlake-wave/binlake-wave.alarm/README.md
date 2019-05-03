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
        调用 RetryTimesAlarmUtils.alarm(long times, String msg)
        ```
    * 其余报警  
        ```sbtshell
        直接调用 AlarmUtils.alarm(String msg)
        ```
        
## 实现方式  
* url post请求   
    采用公司统一的报警接口发送报警短信  

