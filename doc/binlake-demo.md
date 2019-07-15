# binlake-QuickStart 

binlake部署文档示例如下: 

## 准备    
* 虚拟机6台  
    ```text
    192.168.200.151
    192.168.200.152
    192.168.200.153
    192.168.200.154
    192.168.200.155
    192.168.200.156
    192.168.200.157
    ```
    分别用于部署zk,binlake-manager,binlake-wave元数据集群 
   
* 虚拟机环境  
    ```text
    root@192.168.200.151:binlake$ echo $JAVA_HOME
    /export/servers/jdk1.8.0_201
    
    root@192.168.200.151:binlake$ java -version
    java version "1.8.0_201"
    Java(TM) SE Runtime Environment (build 1.8.0_201-b09)
    Java HotSpot(TM) 64-Bit Server VM (build 25.201-b09, mixed mode)
    ```
    每台虚拟几都需要安装jdk环境, 版本要求jdk1.8

## 安装zk  
zookeeper集群部署需要注意配置文件的一致性,以下三个节点用于部署zookeeper   
```text
192.168.200.151
192.168.200.152
192.168.200.153
```

* zoo.cfg 
    ```text
    # The number of milliseconds of each tick
    tickTime=2000
    # The number of ticks that the initial 
    # synchronization phase can take
    initLimit=10
    # The number of ticks that can pass between 
    # sending a request and getting an acknowledgement
    syncLimit=5
    dataDir=/export/servers/zookeeper-3.4.12/data
    logDir=/export/servers/zookeeper-3.4.12/logs
    
    server.1=192.168.200.151:28881:38881
    server.2=192.168.200.152:28881:38881
    server.3=192.168.200.153:28881:38881
    
    autopurge.purgeInterval=24
    minSessionTimeout=10000
    maxSessionTimeout=60000
    #skipACL binlake is not use ACL
    skipACL=yes
    ```
    每个节点(/export/servers/zookeeper-3.4.12/conf)目录下的zoo.cfg 都采用以上配置  

* 启动zookeeper服务  
    ```text
    # 192.168.200.151 
    root@192.168.200.151:zookeeper-3.4.12/bin$ ./bin/zkServer.sh start

    # 192.168.200.152
    root@192.168.200.152:zookeeper-3.4.12/bin$ ./bin/zkServer.sh start
    
    # 192.168.200.153
    root@192.168.200.153:zookeeper-3.4.12/bin$ ./bin/zkServer.sh start
    ```

## 安装binlake-manager 
**binlake-manager**用户连接**binlake-web**与**binlake-wave**  
安装到 **192.168.200.154**节点  
* 下载 binlake-manager, 访问 release 页面 , 选择需要的包下载, 如以 1.0.0 版本为例  
    ```text
    # 下载连接地址 
    https://github.com/jd-tiger/binlake/releases/download/1.0.0/binlake-manager-3.0-server.tar.gz
    ```

* 解压缩  
    ```text
    root@192.168.200.154:/export/servers$ tar zxvf binlake-manager-3.0.tar.gz  -C /export/servers/
    ```
    * 解压完成 看到如下目录结构  
    ```text
    root@192.168.200.154:binlake-manager-3.0$ ll
    total 28
    drwxr-xr-x 6 pengan pengan 4096 7月  15 09:43 ./
    drwxr-xr-x 8 pengan pengan 4096 7月  15 09:43 ../
    drwxr-xr-x 2 pengan pengan 4096 7月  15 09:43 bin/
    drwxr-xr-x 2 pengan pengan 4096 7月  15 09:43 conf/
    drwxr-xr-x 2 pengan pengan 4096 7月  15 09:43 lib/
    drwxr-xr-x 2 pengan pengan 4096 7月  12 17:39 logs/
    -rw-r--r-- 1 pengan pengan 2487 5月  26 15:20 README.md
    ```

* 修改配置  
    ```text
    root@192.168.200.154:binlake-manager-3.0$ vim conf/config.properties
    ```
    修改**binlake-manager**对外访问端口  
    ```text
    # tower serving port
    tower.sever.port=9096
    ```
    
    **这里tower.server.port**对应的**binlake-web/conf/app.conf** manager模块的url

* 启动  
    ```text
    root@192.168.200.154:bin$ ls
    start.sh  stop.sh
    root@192.168.200.154:bin$ sh start.sh 
    ```

* 查看console日志  
    ```text
    root@192.168.200.154:bin$ cd ../logs
    start.sh  stop.sh
    root@192.168.200.154:bin$ tail -f console.log  
    ```
    **binlake-manager**启动日志效果如下  
    ```text
    root@192.168.200.154:logs$ tail -f console.log 
    Java HotSpot(TM) 64-Bit Server VM warning: ignoring option MaxPermSize=256m; support was removed in 8.0
    Java HotSpot(TM) 64-Bit Server VM warning: ignoring option PermSize=256m; support was removed in 8.0
    Java HotSpot(TM) 64-Bit Server VM warning: UseCMSCompactAtFullCollection is deprecated and will likely be removed in a future release.
    log4j:WARN 2019-07-15 09:50:26 [/export/sources/binlake/binlake-manager/target/binlake-manager-3.0//conf/log4j.xml] load completed.
    2019-07-15 09:50:26.196  [INFO ] [main] (TowerServer.java:23) - start
    2019-07-15 09:50:26.202  [INFO ] [main] (ConfigLoader.java:18) - load
    2019-07-15 09:50:26.719  [INFO ] [main] (Server.java:272) - jetty-8.1.13.v20130916
    2019-07-15 09:50:26.886  [INFO ] [main] (AbstractConnector.java:338) - Started SelectChannelConnector@0.0.0.0:9096
    ```
    
* 停止   
    ```text
    root@192.168.200.154:bin$ sh stop.sh   
    ```

* 更多**binlake-manager**咨询   
请查看[binlake-manager](../binlake-manager/README.md)    

## 安装binlake-wave  
**binlake-wave**是dump 数据的核心 部署到以下三台机器当中
```text
192.168.200.155
192.168.200.156
192.168.200.157
```
每台机器对应的操作都采用统一配置, 以下示例以**192.168.200.155**做说明  

* 下载 binlake-wave, 访问 release 页面 , 选择需要的包下载, 如以 1.0.0 版本为例  
    ```text
    # 下载连接地址 
    https://github.com/jd-tiger/binlake/releases/download/1.0.0/binlake-wave.server-3.0-server.tar.gz
    ```

* 解压缩  
    ```text
    root@192.168.200.155:/export/servers$ tar zxvf binlake-wave.server-3.0-server.tar.gz  -C /export/servers/
    ```
    * 解压完成 看到如下目录结构  
    ```text
    root@192.168.200.155:binlake-wave.server-3.0$ ll
    total 28
    drwxr-xr-x  6 pengan pengan 4096 7月  15 09:58 ./
    drwxr-xr-x 10 pengan pengan 4096 7月  15 09:58 ../
    drwxr-xr-x  2 pengan pengan 4096 7月  15 09:58 bin/
    drwxr-xr-x  2 pengan pengan 4096 7月  15 09:58 conf/
    drwxr-xr-x  2 pengan pengan 4096 7月  15 09:58 lib/
    drwxr-xr-x  2 pengan pengan 4096 7月  12 17:39 logs/
    -rw-r--r--  1 pengan pengan 4038 5月   6 16:04 README.md
    ```

* 修改配置  
    ```text
    root@192.168.200.155:binlake-manager-3.0$ vim conf/config.properties
    ```
    修改**binlake-wave**对外访问端口  
    ```text
    ###### start zookeeper configuration ####################3
    zk.path.meta=/zk/wave3
    
    # zk服务集群地址
    zk.servers=192.168.200.151:2181,192.168.200.152:2181,192.168.200.153:2181
    
    # 流控 buffer size
    wave.server.throttle.size=16
    
    # wave 服务处理器个数 
    wave.server.processors=4
    
    # wave http服务端口
    wave.http.server.port=8083
    
    # wave agent服务端口
    wave.agent.server.port=4006
    
    # binlog dump 分布式集群 重试次数
    wave.dump.latch=9
    
    # rpc kill preleader 分布式集群重复次数
    wave.kill.latch=3
    
    # 数据写入zk 定时器timer 的时间间隔 单位 毫秒
    wave.timer.period=60000
    ```
    **wave.http.server.port**是**binlake-wave**集群用户内部通信的端口
    
* 启动  
    ```text
    root@192.168.200.155:bin$ ls
    start.sh  stop.sh
    root@192.168.200.155:bin$ sh start.sh 
    ```

* 查看console日志  
    ```text
    root@192.168.200.155:bin$ cd ../logs
    root@192.168.200.155:bin$ ll 
    total 24
    drwxr-xr-x 2 pengan pengan  4096 7月  15 10:03 ./
    drwxr-xr-x 6 pengan pengan  4096 7月  15 09:58 ../
    -rw-r--r-- 1 pengan pengan 10957 7月  15 10:03 console.log
    -rw-r--r-- 1 pengan pengan     0 7月  15 10:03 debug.log
    -rw-r--r-- 1 pengan pengan     0 7月  15 10:03 error.log
    -rw-r--r-- 1 pengan pengan   644 7月  15 10:03 info.log
    -rw-r--r-- 1 pengan pengan     0 7月  15 10:03 warn.log
    ```
    **binlake-wave**启动日志效果如下  
    ```text
    root@192.168.200.155:logs$ tail -f console.log 
    log4j:WARN 2019-07-15 10:04:52 [/export/sources/binlake/binlake-wave/binlake-wave.server/target/binlake-wave.server-3.0//conf/log4j.xml] load
     completed.
    2019-07-15 10:04:52.951  [INFO ] [main] (CuratorFrameworkImpl.java:235) - Starting
    2019-07-15 10:04:52.961  [INFO ] [main] (Environment.java:100) - Client environment:zookeeper.version=3.4.5-1392090, built on 09/30/2012 17:5
    2 GMT
    2019-07-15 10:04:52.961  [INFO ] [main] (Environment.java:100) - Client environment:host.name=192.168.200.155
    2019-07-15 10:04:52.961  [INFO ] [main] (Environment.java:100) - Client environment:java.version=1.8.0_201
    2019-07-15 10:04:52.961  [INFO ] [main] (Environment.java:100) - Client environment:java.vendor=Oracle Corporation
    2019-07-15 10:04:52.961  [INFO ] [main] (Environment.java:100) - Client environment:java.home=/export/servers/jdk1.8.0_201/jre
    ```
    
* 停止   
    ```text
    root@192.168.200.155:bin$ sh stop.sh   
    ```
 
* 更多**binlake-wave**咨询   
请查看[binlake-wave](../binlake-wave/binlake-wave.server/README.md)  