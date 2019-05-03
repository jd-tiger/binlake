package com.jd.binlog.meta;

import com.jd.binlog.util.GzipUtil;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.List;
import java.util.Map;

/**
 * Created by ninet on 19-1-30.
 */
public class Meta {
    /***************************************
     meta base info
     ****************************************/
    public static class DbInfo {
        private long slaveId;       //slave id
        private String host;        //MySQL host to connect
        private int port;           //MySQL sever port
        private String user;        //MySQL replication user
        private String password;    //MySQL replication password
        private long createTime;    //node created timestamp
        private NodeState state;    //node state
        private List<Rule> rule;    //rule
        private long metaVersion;   //meta version increate if update by manager-server
        private String slaveUUID;   //slave uuid

        public long getSlaveId() {
            return slaveId;
        }

        public DbInfo setSlaveId(long slaveId) {
            this.slaveId = slaveId;
            return this;
        }

        public String getHost() {
            return host;
        }

        public DbInfo setHost(String host) {
            this.host = host;
            return this;
        }

        public int getPort() {
            return port;
        }

        public DbInfo setPort(int port) {
            this.port = port;
            return this;
        }

        public String getUser() {
            return user;
        }

        public DbInfo setUser(String user) {
            this.user = user;
            return this;
        }

        public String getPassword() {
            return password;
        }

        public DbInfo setPassword(String password) {
            this.password = password;
            return this;
        }

        public long getCreateTime() {
            return createTime;
        }

        public DbInfo setCreateTime(long createTime) {
            this.createTime = createTime;
            return this;
        }

        public NodeState getState() {
            return state;
        }

        public DbInfo setState(NodeState state) {
            this.state = state;
            return this;
        }

        public List<Rule> getRule() {
            return rule;
        }

        public DbInfo setRule(List<Rule> rule) {
            this.rule = rule;
            return this;
        }

        public long getMetaVersion() {
            return metaVersion;
        }

        public DbInfo setMetaVersion(long metaVersion) {
            this.metaVersion = metaVersion;
            return this;
        }

        public String getSlaveUUID() {
            return slaveUUID;
        }

        public DbInfo setSlaveUUID(String slaveUUID) {
            this.slaveUUID = slaveUUID;
            return this;
        }

        public static byte[] marshalJson(DbInfo dbInfo) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(dbInfo));
        }

        public static DbInfo unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), DbInfo.class);
        }
    }

    /***************************************
     why separate the meta info into two parts?
     the base information upper is read only in most circumstance
     this lower information BinlogInfo writes often so let them alone

     meta dynamic info that have to update often
     ****************************************/
    public static class BinlogInfo {
        private String binlogFile;          //MySQL replication binlog file name
        private long binlogPos;             //MySQL replication binlog position
        private String executedGtidSets;    //MySQL replication with gtid sets
        private String leader;              //leader info
        private long leaderVersion;         //version for binlog info first set to 0
        private long binlogWhen;            //binlog dump timestamp : when
        private long transactionId;
        private String instanceIp;          //ip in relation to MySQL instance
        private boolean withGTID;
        private String preLeader;           //previous leader
        private Map<String, Long> msgid;    //table message id

        public String getBinlogFile() {
            return binlogFile;
        }

        public BinlogInfo setBinlogFile(String binlogFile) {
            this.binlogFile = binlogFile;
            return this;
        }

        public long getBinlogPos() {
            return binlogPos;
        }

        public BinlogInfo setBinlogPos(long binlogPos) {
            this.binlogPos = binlogPos;
            return this;
        }

        public String getExecutedGtidSets() {
            return executedGtidSets;
        }

        public BinlogInfo setExecutedGtidSets(String executedGtidSets) {
            this.executedGtidSets = executedGtidSets;
            return this;
        }

        public String getLeader() {
            return leader;
        }

        public BinlogInfo setLeader(String leader) {
            this.leader = leader;
            return this;
        }

        public long getLeaderVersion() {
            return leaderVersion;
        }

        public BinlogInfo setLeaderVersion(long leaderVersion) {
            this.leaderVersion = leaderVersion;
            return this;
        }

        public long getBinlogWhen() {
            return binlogWhen;
        }

        public BinlogInfo setBinlogWhen(long binlogWhen) {
            this.binlogWhen = binlogWhen;
            return this;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public BinlogInfo setTransactionId(long transactionId) {
            this.transactionId = transactionId;
            return this;
        }

        public String getInstanceIp() {
            return instanceIp;
        }

        public BinlogInfo setInstanceIp(String instanceIp) {
            this.instanceIp = instanceIp;
            return this;
        }

        public boolean getWithGTID() {
            return withGTID;
        }

        public BinlogInfo setWithGTID(boolean withGTID) {
            this.withGTID = withGTID;
            return this;
        }

        public String getPreLeader() {
            return preLeader;
        }

        public BinlogInfo setPreLeader(String preLeader) {
            this.preLeader = preLeader;
            return this;
        }

        public Map<String, Long> getMsgid() {
            return msgid;
        }

        public BinlogInfo setMsgid(Map<String, Long> msgid) {
            this.msgid = msgid;
            return this;
        }

        public static byte[] marshalJson(BinlogInfo binlogInfo) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(binlogInfo));
        }

        public static BinlogInfo unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), BinlogInfo.class);
        }
    }

    /**
     counter is save retry times
     **/
    public static class Counter {
        private long retryTimes;    //times that Wave Server try to connect MySQL server
        private long killTimes;     //times that Wave Server try to connect MySQL server

        public long getRetryTimes() {
            return retryTimes;
        }

        public Counter setRetryTimes(long retryTimes) {
            this.retryTimes = retryTimes;
            return this;
        }

        public long getKillTimes() {
            return killTimes;
        }

        public Counter setKillTimes(long killTimes) {
            this.killTimes = killTimes;
            return this;
        }

        public static byte[] marshalJson(Counter counter) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(counter));
        }

        public static Counter unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Counter.class);
        }
    }

    public static class BindLeaders {
        private List<Pair> leaders;
        private long retryTimes;

        public List<Pair> getLeaders() {
            return leaders;
        }

        public BindLeaders setLeaders(List<Pair> leaders) {
            this.leaders = leaders;
            return this;
        }

        public long getRetryTimes() {
            return retryTimes;
        }

        public BindLeaders setRetryTimes(long retryTimes) {
            this.retryTimes = retryTimes;
            return this;
        }

        public static byte[] marshalJson(BindLeaders bindLeaders) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(bindLeaders));
        }

        public static BindLeaders unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), BindLeaders.class);
        }
    }

    public static class Terminal {
        private String binlogFile;
        private long binlogPos;
        private String gtid;
        private List<String> newHost;   //对应 reshard  之后新的host

        public String getBinlogFile() {
            return binlogFile;
        }

        public Terminal setBinlogFile(String binlogFile) {
            this.binlogFile = binlogFile;
            return this;
        }

        public long getBinlogPos() {
            return binlogPos;
        }

        public Terminal setBinlogPos(long binlogPos) {
            this.binlogPos = binlogPos;
            return this;
        }

        public String getGtid() {
            return gtid;
        }

        public Terminal setGtid(String gtid) {
            this.gtid = gtid;
            return this;
        }

        public List<String> getNewHost() {
            return newHost;
        }

        public Terminal setNewHost(List<String> newHost) {
            this.newHost = newHost;
            return this;
        }

        public static byte[] marshalJson(Terminal terminal) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(terminal));
        }

        public static Terminal unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Terminal.class);
        }
    }

    public static class Candidate {
        private List<String> host;

        public List<String> getHost() {
            return host;
        }

        public Candidate setHost(List<String> host) {
            this.host = host;
            return this;
        }

        public static byte[] marshalJson(Candidate candidate) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(candidate));
        }

        public static Candidate unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Candidate.class);
        }
    }

    /***
     ** 规则 包括规则类型 具体的规则 mq/kv/etc.
     *****/
    public static class Rule {
        private StorageType storage;    //存储类型
        private String convertClass;    //消息格式转换器类名
        private byte[] any;             //存储类型对应的具体参数， 规则， 类型使用方式 etc..

        public StorageType getStorage() {
            return storage;
        }

        public void setStorage(StorageType storage) {
            this.storage = storage;
        }

        public String getConvertClass() {
            return convertClass;
        }

        public void setConvertClass(String convertClass) {
            this.convertClass = convertClass;
        }

        public byte[] getAny() {
            return any;
        }

        public void setAny(byte[] any) {
            this.any = any;
        }

        public static byte[] marshalJson(Rule rule) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(rule));
        }

        public static Rule unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Rule.class);
        }
    }

    /**
     topic rules map :

     one topic in relation to many rules

     one rule in relation to many topic

     **/
    public static class MQRule {
        private String topic;               //mq topic
        private boolean withTransaction;    //是否携带事务信息 begin or commit message
        private boolean withUpdateBefore;   //update事件信息是否携带变更前数据
        private String producerClass;       //producer class name with constructor parameter List<Meta.Pair> paras
        private OrderType type;             //topic类型：顺序主题或是非顺序主题
        private List<Pair> para;            //MQ 链接参数
        private List<Filter> white;         //白名单
        private List<Filter> black;         //黑名单

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public boolean isWithTransaction() {
            return withTransaction;
        }

        public void setWithTransaction(boolean withTransaction) {
            this.withTransaction = withTransaction;
        }

        public boolean isWithUpdateBefore() {
            return withUpdateBefore;
        }

        public void setWithUpdateBefore(boolean withUpdateBefore) {
            this.withUpdateBefore = withUpdateBefore;
        }

        public String getProducerClass() {
            return producerClass;
        }

        public void setProducerClass(String producerClass) {
            this.producerClass = producerClass;
        }

        public OrderType getType() {
            return type;
        }

        public void setType(OrderType type) {
            this.type = type;
        }

        public List<Pair> getPara() {
            return para;
        }

        public void setPara(List<Pair> para) {
            this.para = para;
        }

        public List<Filter> getWhite() {
            return white;
        }

        public void setWhite(List<Filter> white) {
            this.white = white;
        }

        public List<Filter> getBlack() {
            return black;
        }

        public void setBlack(List<Filter> black) {
            this.black = black;
        }

        public static byte[] marshalJson(MQRule mqRule) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(mqRule));
        }

        public static MQRule unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), MQRule.class);
        }
    }

    public static class Filter {
        private String table;
        private List<EventType> eventType;
        private List<Column> white;
        private List<Column> black;
        private List<Pair> fakeColumn;
        private List<String> hashKey;

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public List<EventType> getEventType() {
            return eventType;
        }

        public void setEventType(List<EventType> eventType) {
            this.eventType = eventType;
        }

        public List<Column> getWhite() {
            return white;
        }

        public void setWhite(List<Column> white) {
            this.white = white;
        }

        public List<Column> getBlack() {
            return black;
        }

        public void setBlack(List<Column> black) {
            this.black = black;
        }

        public List<Pair> getFakeColumn() {
            return fakeColumn;
        }

        public void setFakeColumn(List<Pair> fakeColumn) {
            this.fakeColumn = fakeColumn;
        }

        public List<String> getHashKey() {
            return hashKey;
        }

        public void setHashKey(List<String> hashKey) {
            this.hashKey = hashKey;
        }

        public static byte[] marshalJson(Filter filter) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(filter));
        }

        public static Filter unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Filter.class);
        }
    }

    public static class Column {
        private String name;
        private List<String> value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getValue() {
            return value;
        }

        public void setValue(List<String> value) {
            this.value = value;
        }
    }

    public static class Pair {
        private String key;
        private String value;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public enum NodeState {
        ONLINE,
        OFFLINE;
    }

    /** 事件类型 **/
    public enum EventType {
        OTHER,
        INSERT,
        UPDATE,
        DELETE,
        CREATE,
        ALTER,
        ERASE,
        QUERY,
        TRUNCATE,
        RENAME,
        CINDEX,
        DINDEX;
    }

    /** 消息顺序类型 **/
    public enum OrderType {
        NO_ORDER,           //完全乱序 无规则
        BUSINESS_KEY_ORDER, //业务主键级别消息顺序 partition 对应多个
        TABLE_ORDER,        //表级别消息顺序 partition 对应多个
        TRANSACTION_ORDER,  //事务消息级别顺序
        DB_ORDER,           //库级别消息 顺序
        INSTANCE_ORDER;     //实例级别消息顺序 broker 对应一个
    }

    /** 存储类型 **/
    public enum StorageType {
        MQ_STORAGE, //消息队列 规则
        KV_STORAGE; //KV storage
    }

    /** zookeeper 信息结构体 **/
    public static class ZK {
        private String servers; //zk servers地址
        private String path;    //监听的根路径

        public String getServers() {
            return servers;
        }

        public ZK setServers(String servers) {
            this.servers = servers;
            return this;
        }

        public String getPath() {
            return path;
        }

        public ZK setPath(String path) {
            this.path = path;
            return this;
        }
    }

    /** MetaData 页面与管理端交互用的元数据信息 **/
    public static class MetaData {
        private DbInfo dbInfo;
        private BinlogInfo slave;
        private BinlogInfo master;
        private Counter counter;
        private Terminal terminal;
        private List<String> candidate;
        private ZK zk;

        public DbInfo getDbInfo() {
            return dbInfo;
        }

        public void setDbInfo(DbInfo dbInfo) {
            this.dbInfo = dbInfo;
        }

        public BinlogInfo getSlave() {
            return slave;
        }

        public void setSlave(BinlogInfo slave) {
            this.slave = slave;
        }

        public BinlogInfo getMaster() {
            return master;
        }

        public void setMaster(BinlogInfo master) {
            this.master = master;
        }

        public Counter getCounter() {
            return counter;
        }

        public void setCounter(Counter counter) {
            this.counter = counter;
        }

        public Terminal getTerminal() {
            return terminal;
        }

        public void setTerminal(Terminal terminal) {
            this.terminal = terminal;
        }

        public List<String> getCandidate() {
            return candidate;
        }

        public void setCandidate(List<String> candidate) {
            this.candidate = candidate;
        }

        public ZK getZk() {
            return zk;
        }

        public void setZk(ZK zk) {
            this.zk = zk;
        }

        public static byte[] marshalJson(MetaData metaData) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(metaData));
        }

        public static MetaData unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), MetaData.class);
        }
    }
}
