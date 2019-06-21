package com.jd.binlog.meta;

import com.jd.binlog.util.GzipUtil;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Arrays;
import java.util.LinkedList;
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
     * counter is save retry times
     **/
    public static class Counter {
        private int retryTimes;    //times that Wave Server try to connect MySQL server
        private int killTimes;     //times that Wave Server try to connect MySQL server

        public int getRetryTimes() {
            return retryTimes;
        }

        public Counter setRetryTimes(int retryTimes) {
            this.retryTimes = retryTimes;
            return this;
        }

        public int getKillTimes() {
            return killTimes;
        }

        public Counter setKillTimes(int killTimes) {
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

    /**
     * error node to remember error information
     */
    public static class Error {
        private int code;
        private byte[] msg;

        public int getCode() {
            return code;
        }

        public Error setCode(int code) {
            this.code = code;
            return this;
        }

        public byte[] getMsg() {
            return msg;
        }

        public Error setMsg(byte[] msg) {
            this.msg = msg;
            return this;
        }

        public static byte[] marshalJson(Error err) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(err));
        }

        public static Error unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Error.class);
        }

        /***
         * default error
         * @return
         */
        public static Error defalut() {
            Meta.Error err = new Error();
            err.code = 0;
            err.msg = "".getBytes();
            return err;
        }

        @Override
        public String toString() {
            return "Error{" +
                    "code=" + code +
                    ", msg=" + Arrays.toString(msg) +
                    '}';
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
     * topic rules map :
     * <p>
     * one topic in relation to many rules
     * <p>
     * one rule in relation to many topic
     **/
    public static class MQRule {
        private String topic;               //mq topic
        private boolean withTransaction;    //是否携带事务信息 begin or commit message
        private boolean withUpdateBefore;   //update事件信息是否携带变更前数据
        private String producerClass;       //producer class name with constructor parameter List<Meta.Pair> mail
        private OrderType order;             //topic类型：顺序主题或是非顺序主题
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

        public OrderType getOrder() {
            return order;
        }

        public void setOrder(OrderType order) {
            this.order = order;
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

        @Override
        public String toString() {
            return "Pair{" +
                    "key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }

    public enum NodeState {
        ONLINE,
        OFFLINE;
    }

    /**
     * 事件类型
     **/
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

    /**
     * 消息顺序类型
     **/
    public enum OrderType {
        NO_ORDER,           //完全乱序 无规则
        BUSINESS_KEY_ORDER, //业务主键级别消息顺序 partition 对应多个
        TABLE_ORDER,        //表级别消息顺序 partition 对应多个
        TRANSACTION_ORDER,  //事务消息级别顺序
        DB_ORDER,           //库级别消息 顺序
        INSTANCE_ORDER;     //实例级别消息顺序 broker 对应一个
    }

    /**
     * 存储类型
     **/
    public enum StorageType {
        MQ_STORAGE, //消息队列 规则
        KV_STORAGE; //KV storage
    }

    /**
     * zookeeper 信息结构体
     **/
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

    /**
     * MetaData 页面与管理端交互用的元数据信息
     **/
    public static class MetaData {
        private DbInfo dbInfo;
        /***
         * slave node : mysql dump status offset
         */
        private BinlogInfo slave;
        /***
         * master for node: MySQL show master status
         */
        private BinlogInfo master;
        /***
         * counter node
         */
        private Counter counter;
        /**
         * terminal node
         */
        private Terminal terminal;
        private List<String> candidate;
        /***
         * alarm node
         */
        private Alarm alarm;

        /***
         * error path
         */
        private Error error;

        /***
         * admin node
         */
        private Admin admin;

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

        public Alarm getAlarm() {
            return alarm;
        }

        public void setAlarm(Alarm alarm) {
            this.alarm = alarm;
        }

        public Error getError() {
            return error;
        }

        public void setError(Error error) {
            this.error = error;
        }

        public Admin getAdmin() {
            return admin;
        }

        public MetaData setAdmin(Admin admin) {
            this.admin = admin;
            return this;
        }

        public static byte[] marshalJson(MetaData metaData) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(metaData));
        }

        public static MetaData unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), MetaData.class);
        }
    }

    /**
     * alarm 节点示例节点位于统一层次 因为一个实例下所有数据共享
     */
    public static class Alarm {
        /**
         * users
         */
        private List<User> users;

        /***
         * retry times latch
         */
        private int retry = 9;

        /**
         * kill time latch
         */
        private int kill = 3;

        public List<User> getUsers() {
            return users;
        }

        public Alarm setUsers(List<User> users) {
            this.users = users;
            return this;
        }

        public int getRetry() {
            return retry;
        }

        public Alarm setRetry(int retry) {
            this.retry = retry;
            return this;
        }

        public int getKill() {
            return kill;
        }

        public Alarm setKill(int kill) {
            this.kill = kill;
            return this;
        }

        @Override
        public String toString() {
            return "Alarm{" +
                    "users=" + users +
                    ", retry=" + retry +
                    ", kill=" + kill +
                    '}';
        }

        public static byte[] marshalJson(Alarm alarm) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(alarm));
        }

        public static Alarm unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Alarm.class);
        }

        /**
         * take default alarm
         *
         * @return
         */
        public static Alarm defalut() {
            Alarm alarm = new Alarm();
            alarm.users = new LinkedList<>();
            alarm.users.add(User.defalut());
            return alarm;
        }
    }

    /**
     * User 用户信息 用于报警以及短信发送
     */
    public static class User {
        /**
         * phone number
         */
        private String phone;

        /***
         * email address
         */
        private String email;

        public String getPhone() {
            return phone;
        }

        public User setPhone(String phone) {
            this.phone = phone;
            return this;
        }

        public String getEmail() {
            return email;
        }

        public User setEmail(String email) {
            this.email = email;
            return this;
        }

        @Override
        public String toString() {
            return "User{" +
                    "phone='" + phone + '\'' +
                    ", email='" + email + '\'' +
                    '}';
        }

        public static byte[] marshalJson(User user) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(user));
        }

        public static User unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), User.class);
        }

        /***
         * default user if not exist
         * @return
         */
        public static User defalut() {
            return new User().setEmail("pengan@jd.com").setPhone("18515819096");
        }
    }

    /**
     * Admin 管理员节点 用于保存 报警请求信息
     */
    public static class Admin {
        Map<String, String> mail; // http mail parameters for post request

        Map<String, String> phone; // http phone parameters for post request

        List<User> users; // admin users information

        public Map<String, String> getMailParas() {
            return mail;
        }

        public Admin setMail(Map<String, String> mail) {
            this.mail = mail;
            return this;
        }

        public List<User> getUsers() {
            return users;
        }

        public Admin setUsers(List<User> users) {
            this.users = users;
            return this;
        }

        public Map<String, String> getPhoneParas() {
            return phone;
        }

        public void setPhone(Map<String, String> phone) {
            this.phone = phone;
        }

        @Override
        public String toString() {
            return "Admin{" +
                    "mail=" + mail +
                    ", phone=" + phone +
                    ", users=" + users +
                    '}';
        }

        public static byte[] marshalJson(Admin admin) throws Exception {
            return GzipUtil.compress(new ObjectMapper().writeValueAsBytes(admin));
        }

        public static Admin unmarshalJson(byte[] json) throws Exception {
            return new ObjectMapper().readValue(GzipUtil.uncompress(json), Admin.class);
        }

        /***
         * get admin mails
         * @return
         */
        public String[] getAdminMails() {
            String[] ms = new String[users.size()];
            for (int i = 0; i < ms.length; i++) {
                ms[i] = users.get(i).getEmail();
            }
            return ms;
        }

        /***
         * get admin phones
         * @return
         */
        public String[] getAdminPhones() {
            String[] ps = new String[users.size()];
            for (int i = 0; i < ps.length; i++) {
                ps[i] = users.get(i).getPhone();
            }
            return ps;
        }
    }
}
