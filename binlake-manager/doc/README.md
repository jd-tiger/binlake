# Binlake-Manager 
Binlake 管理端控制器, 控制由前端页面下发的命令, 避免前端直接修改zk当中元数据,影响系统的稳定性  

manager只是与**zk**元数据交互, 不会直接参与dump服务的切换,以及leader选举情况


## Interface 
路径 | 作用 | 参数类型 | 返回值类型
--- | --- | --- | --- 
/create/znodes/ | 创建一个ZNode{对应到一个MySQL实例}| MetaData | {"code": 1000, "message":""}
/set/binlog/ | 设置binlog 位置{binlog-file, binlog-pos, gtid} | MetaData | {"code": 1000, "message":""}
/set/online/ | 上线MySQL节点 开始dump数据 | MetaData | {"code": 1000, "message":""}
/set/offline/ | 下线MySQL 节点 停止dump 数据 | MetaData | {"code": 1000, "message":""}
/set/leader/ | 设置MySQL dump 服务节点的leader  | MetaData | {"code": 1000, "message":""}
/set/candidate/ | 设置MySQL dump 服务的备选 节点  | MetaData | {"code": 1000, "message":""}
/set/terminal/ | 设置终止节点 | MetaData | {"code": 1000, "message":""}
/reset/counter/ | 重置计数器{retryTimes = 0} | MetaData | {"code": 1000, "message":""}
/rollback/ | 回滚到任意时间点  | MetaData | {"code": 1000, "message":""}
/slave/status | 获取wave dump 完成的binlog 位置 | MetaData | MetaData









