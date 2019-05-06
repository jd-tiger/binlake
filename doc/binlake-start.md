# binlake-start  
介绍binlake如何编译部署到生产环境, 分两部分进行说明： tower的使用文档说明, 服务的编译部署


## tower使用文档说明  
[**tower使用文档**](../tower/README.md)  

## 服务编译部署  
[**Binlake-manager编译**](../binlake-manager/README.md#编译)  

[**Binlake-wave 编译**](../binlake-wave/binlake-wave.server/README.md#编译)

### 运行manager以及wave-service    

#### 运行zk集群  
* 配置运行zk集群    
    参考 [**zk文档**](https://zookeeper.apache.org/doc/r3.4.14/zookeeperAdmin.html)

* 配置zk集群域名  
    对zk集群配置相应的域名供外部访问  
    ```text
    zh.binlake.jd.com  解析的ip包括{192.168.200.156, 192.168.200.157, 192.168.200.158}
    ```

#### 运行wave-service集群  
* 配置wave-service  
    [**Binlake-server参数配置**](../binlake-wave/binlake-wave.server/README.md#参数配置)

#### 运行binlake-manager  
* 配置binlake-manager  
    [**BinLake-manager参数配置**](../binlake-manager/README.md#参数配置)  

#### tower管理端初始化    
[BinLake-tower初始化](../tower/tower-init.md)    

#### tower操作手册  
[BinLake-tower操作手册](../tower/tower-handbook.md)    

