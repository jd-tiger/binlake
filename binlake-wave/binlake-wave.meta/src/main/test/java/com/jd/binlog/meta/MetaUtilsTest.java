package com.jd.binlog.meta;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MetaUtilsTest {
    public static void main(String[] args) throws Exception {
        List<Meta.EventType> evs = new LinkedList<>();
        evs.add(Meta.EventType.CINDEX);
        evs.add(Meta.EventType.CREATE);
        evs.add(Meta.EventType.DELETE);
        evs.add(Meta.EventType.INSERT);
        evs.add(Meta.EventType.UPDATE);

        Meta.Filter f1 = new Meta.Filter();
        f1.setTable("order[\\d]+.tb[\\d]+");
        f1.setEventType(evs);

        Meta.Filter f2 = new Meta.Filter();
        f2.setTable("sys[\\d]+.tb[\\d]+");
        f2.setEventType(evs);

        List<Meta.Filter> fs = new LinkedList<>();
        fs.add(f1);
        fs.add(f2);

        Meta.MQRule mr1 = new Meta.MQRule();
        mr1.setTopic("binlake1");
        mr1.setType(Meta.OrderType.BUSINESS_KEY_ORDER);
        mr1.setWithTransaction(false);
        mr1.setWithUpdateBefore(false);
        mr1.setBlack(fs);

        Meta.MQRule mr2 = new Meta.MQRule();
        mr2.setTopic("binlake2");
        mr2.setType(Meta.OrderType.BUSINESS_KEY_ORDER);
        mr2.setWithTransaction(false);
        mr2.setWithUpdateBefore(false);
        mr2.setBlack(fs);

        Meta.Rule r1 = new Meta.Rule();
        r1.setConvertClass("com.jd.binlog.convert.ProtobufConvert");
        r1.setStorage(Meta.StorageType.MQ_STORAGE);
        r1.setAny(Meta.MQRule.marshalJson(mr1));

        Meta.Rule r2 = new Meta.Rule();
        r2.setConvertClass("com.jd.binlog.convert.ProtobufConvert");
        r2.setStorage(Meta.StorageType.MQ_STORAGE);
        r2.setAny(Meta.MQRule.marshalJson(mr2));

        List<Meta.Rule> mrs = new LinkedList<>();
        mrs.add(r1);
        mrs.add(r2);

        Meta.DbInfo dbInfo = new Meta.DbInfo();
        dbInfo.setHost("127.0.0.1").setPort(3358).setSlaveId(1000).setSlaveUUID("ba57e498-4b08-4e73-8e32-943696629378").setUser("root").setPassword("secret").setState(Meta.NodeState.ONLINE);
        dbInfo.setRule(mrs);

        for (Map.Entry<String, byte[]> entry : MetaUtils.dbBytesMap("/zk/wave3", dbInfo).entrySet()) {
            System.err.println(entry.getKey());
        }
    }
}
