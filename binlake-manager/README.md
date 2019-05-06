# binlake-manager   

binlake manager 类似于controller的角色，控制中控tower 与 zk的交互且服务无状态  

## api介绍  
Binlake-Manager Api Document

* api route table 

api route | api handler | api function
 --- | --- | ---
/set/leader | BindLeaderHandler | 客户化定制绑定MySQL instance Leader
/create/znodes | CreateZNodesHandler | create nodes on zookeeper 
/remove/node | RemoveNodeHandler | delete MySQL node on zookeeper using host & port 
/get/slave | GetSlaveBinlogHandler | 获取Dump binlog offset信息
/reset/binlogs | ResetBinlogsHandler | 重置binlog 位置
/reset/counter | ResetCounterHandler | 重置计数器
/set/binlog | SetBinlogPosHandler | 设置binlog的位置 不能设置gtid 
/set/candidate | SetCandidateHandler | 设置dump MySQL节点的candidate wave ips
/set/offline | SetInstanceOffLIne | 设置MySQL 节点dump 下线
/set/online | SetInstanceOnLine | 设置MySQL 节点dump 上线
/set/terminal | SetTerminalHandler | 设置终止节点信息

* paras   
    all paras are using MetaData class json object is ok!

## 使用说明 
binlake-manger使用说明包括编译, 修改配置文件以及部署  

### 编译  
binlake-manager服务依赖binlake-wave中common、以及meta包

* 编译说明  
    服务编译部署包括 binlake-manger
    
    * jdk 使用1.7以上版本  
        因为jdk1.7 以上才开始使用g1 gc算法 
    
    * 安装maven 2.3以上版本  
        设置环境变量 
        ```text
        pengan@pengan:binlake$ alias mvn
        alias mvn='/export/servers/apache-maven-3.6.0/bin/mvn --settings=/export/servers/apache-maven-3.6.0/conf/settings.xml'
        ```
    
    * 下载源码  
    ```text
    git clone https://github.com/jd-tiger/binlake
    ```
    
    * 编译打包    
    ```text
    mvn install -Dmaven.test.skip=true
    # 获取wave service安装包 binlake-manager/target/binlake-manager-3.0-server.tar.gz  
    ```

### 参数配置  
binlake-manager由于不与任何服务之间耦合, 仅仅作为命令的传递与拦截 配置相对简单 

下面表格介绍conf/config.properties 参数   

参数说明 | 含义 | 默认值 
:--- | :--- | :--- 
tower.server.port | tower serving port | 9096

## 说明 
多个manager需要配置统一的端口, 申请统一的域名, 负载均衡的同时也便于tower的统一访问  
