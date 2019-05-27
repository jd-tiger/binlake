# binlake-qa 

## 产品对比  

* 对比开源产品, 又有什么优势  
    开源产品在一定程度上解决公司发展前期的功能, 但随着业务的推广, 集群规模扩大, 我们需要一套新的产品来解决当前的问题[**详细说明**](../README.md) **背景**章节

* 如果与canal合并是否可以  
    不建议    
    两个产品有类似的功能以及相同的数据处理流程, 并且都是解决订阅 数据源MySQL(canal可能支持更多) 增量日志问题  
    对于MySQL binlog 解析 BinLake也是直接引用的canal [**引用详情**](./binlake-quote.md)   
    但是BinLake重心在优化资源使用, 数据流集群的管理, 分布式场景下如何能够继续保持服务的高效稳定  
    

## 产品使用  
* BinLake 是否可以直接拿来就用, 还是需要做一些修改?   
    BinLake 各个模块尽量在与任何的业务解耦, 希望能够提高系统的实用性, 但是比较遗憾, 在报警模块以及生产模块由于各个公司参数形式不统一或者存在多样性目前还无法从中控 **BinLake-web** 下传, 所以使用方还需要修改以下3处:     
    * [JDMailParas](../binlake-wave/binlake-wave.alarm/src/main/java/com/jd/binlog/alarm/utils/JDMailParas.java)  
        需要修改邮件报警的参数 [详见](../binlake-wave/binlake-wave.alarm/README.md)    

    * [JDPhoneParas](../binlake-wave/binlake-wave.alarm/src/main/java/com/jd/binlog/alarm/utils/JDPhoneParas.java)  
        修改短信报警的参数  [详见](../binlake-wave/binlake-wave.alarm/README.md)    

    * [Kafka100Producer](../binlake-wave/binlake-wave.produce/src/main/java/com/jd/binlog/produce/mq/impl/Kafka100Producer.java)   
        kafka客户端 目前采用 kafka-1.0.0版本, 公司内部**MQ** 目前支持的**Kafka** 版本也限于**1.0.0**版本, BinLake 团队已经将最初是的内部MQ客户端完全改造采用通用的 **Kafka** 客户端  [详见](../binlake-wave/binlake-wave.produce/README.md)          
    以上三点, 是需要各个使用方根据自已公司内部的具体业务规则来进行处理, 同时BinLake 团队也尽量升级系统解耦业务端模块, 保持系统的独立  
