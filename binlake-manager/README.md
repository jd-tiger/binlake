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
/rollback | RollbackHandler | 根据时间戳回滚binlog 
/set/binlog | SetBinlogPosHandler | 设置binlog的位置 不能设置gtid 
/set/candidate | SetCandidateHandler | 设置dump MySQL节点的candidate wave ips
/set/offline | SetInstanceOffLIne | 设置MySQL 节点dump 下线
/set/online | SetInstanceOnLine | 设置MySQL 节点dump 上线
/set/terminal | SetTerminalHandler | 设置终止节点信息

* paras   
    all paras are using MetaData class json object is ok!

## StartUp 
* compile 

* update config.properties 
    set server port then using bin/start.sh
 
