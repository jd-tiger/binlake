# binlake-quote  
感谢**canal**对开源社区的贡献

binlake的代码当中有一部分代码来源于开源社区
下面介绍binlake的开源引用项目, 以及对哪些工具包做了修正

## dbsync模块  

dbsync 模块 除以下指出的两个不同的地方外, 其余的关于binlog事件解析与**canal**完全一样

* 模块的对应关系
    canal 中[dbsync](https://github.com/alibaba/canal/tree/master/dbsync)模块对应到 [binlake-wave.dbsync](../binlake-wave/binlake-wave.dbsync) 模块

* 修改的class  
    * [GtidLogEvent.class](../binlake-wave/binlake-wave.dbsync/src/main/java/com/jd/binlog/dbsync/event/GtidLogEvent.java)  
        对于gtid sid的获取采用 HexUtils byte 转 uuid方式统一处理    
    * [LogPosition.class](../binlake-wave/binlake-wave.dbsync/src/main/java/com/jd/binlog/dbsync/LogPosition.java)  
        position 增加isCommit, 引用计数器, gtidset, 并增加对gtidset的refresh操作以及合并操作  


## filter模块　　
filter模块主要作用在与过滤消息; 在binlake项目当中, 由于消息过滤与rule绑定, 在filter模块增加了对接口 **IFilter** 以及 **IRule**实现 

* aviator 模块  
    整个过滤表信息模块采用正则表达式的方式过滤 这块参考 [**canal aviator**](https://github.com/alibaba/canal/tree/master/filter/src/main/java/com/alibaba/otter/canal/filter/aviater) 

## protocol模块 
protocol模块 与 [canal-protocol](https://github.com/alibaba/canal/tree/master/protocol) 包的作用完全类似, proto文件仅仅是命名方式不一致

proto消息格式canal有很好的扩展, 也是考虑产品的通用以及现有业务方使用代价

## 说明 
* canal 引入的的包名是否做了更改    
    统一的包名更方便代码的管理以及维护, 包名已经做了更改, 引入的是源码, 依据的是canal.license apache2.0的license 协议  
    dbsync 这部分代码是兼容MySQL replication协议, 也是为后续增加对自己的数据库replication协议的支持 所以直接引用的是源码   
    

## 感谢  
感谢 [**canal**](https://github.com/alibaba/canal)开源, 我们也继续尊崇延续开源.  