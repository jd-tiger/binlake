# binlake-wave.server 

binlake服务端，负责组合各个模块功能，异常关闭处理，消息处理流程等核心过程等.

## 消息处理流程  

消息处理过程分为多种顺序类型，包括严格实例顺序，库顺序，表顺序，业务主键顺序，事务顺序 等.

![image](./doc/binlake-process.png)


## 异常处理  
异常问题包括，MySQL dump io断开、切换，zk断开，mq发送失败，数据包错误 等. 

![image](./doc/MySQL-Reconnect.jpg)
