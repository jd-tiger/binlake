# binlake server 

binlake server端， 负责集群的报警，消费MQ客户端依赖包，公用模块，加载配置模块，消息格式转换器模块，数据同步解析模块，数据过滤模块，监控模块，模块化之间依赖的接口，元数据模块，性能模块，生产模块，消息协议模块，启动模块，zk交互模块 等10多个模块，模块之间的调用尽量采用接口的方式实现。  

## 模块介绍 

* 公用模块  
    公用模块主要解决模块之间通用的method的实现，统一管理，统一调用

* 配置加载模块  
    配置加载模块解决加载配置文件，配置校验，配置统一生效

* 消息格式转换模块  
    消息格式转换模块负责消息的转换，目前包含protobuf格式，以及avro格式都是完全根据接口实现

* 数据同步解析模块  
    当前模块应用的是canal的[**license**](https://github.com/alibaba/canal/tree/master/dbsync), 但为了jar包名称统一， 我们一句canal 的license协议，修改了包名，并解决早起canal再切换过程当中无法[kill MySQL dump connection](https://github.com/alibaba/canal/issues/284) 等使用当中的异常。 

* 数据过滤模块  
    过滤模块[aviater](binlake-wave.filter/src/main/java/com/jd/binlog/filter/aviater)直接采用的canal的过滤器，过滤的实现有正则表达式的高效，这块实现已经很优秀
    thanks to the power man jianghang
   
* 监控模块  
    监控模块目标是获取监控数据，并且根据将监控数据进行日志写入到文件当中或者直接上传， 目前实现的方式是异步写入文件， 这个包蛀牙用于性能测试  

* 接口模块  
    接口模块是将基本上几乎所有模块之间依赖的接口定义都在interface 模块当中，用于解除包与包之间的强依赖关系

* 元数据模块  
    元数据模块负责元数据的存储，更新，替换，以及refresh等操作  

* 性能模块  
    性能模块负责性能检测

* 生产模块  
    生产模块负责消息的生产转换 等

* 消息协议模块  
    消息协议模块是依赖于[protocol](https://github.com/alibaba/canal/tree/master/protocol)源于canal
    thanks to canal 因为protobuf消息的格式的定义相当完善, 为大数据流式数据处理， binlake也支持avro格式， 后者其他任何消息格式， 需要的就是实现Convert接口  

* 启动模块  
    启动模块负责sever启动， 模块之间和合作调度 etc. 
    
* zk交互模块  
    zk交互模块，解决元数据的更新，leaderselector选主操作 以及 如何与http module协作的问题
