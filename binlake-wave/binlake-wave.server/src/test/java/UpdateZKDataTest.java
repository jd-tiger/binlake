//import com.jd.binlog.meta.Meta;
//import org.apache.curator.framework.CuratorFramework;
//import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.retry.RetryNTimes;
//
//import java.io.File;
//
///**
// * Created by pengan on 17-3-8.
// */
//public class UpdateZKDataTest {
//    private static final String ZK_PATH = "/zk/wave";
//    public static void main(String[] args) {
//        CuratorFramework client = CuratorFrameworkFactory.newClient(
//                "192.168.200.157:2181",
//                new RetryNTimes(3, 6000)
//        );
//        client.start();
//        String host = "jdptest1.jddb.com";
//        try {
//            byte[] data = client.getData().forPath(ZK_PATH + File.separator + host);
//            Meta.DbInfo dbInfo = Meta.DbInfo.parseFrom(data);
//            System.err.println(dbInfo);
//            Meta.TopicRules.Builder topicBuilder = Meta.TopicRules.newBuilder();
//            topicBuilder.setTopic("chihuabin1488854098");
////            topicBuilder.addWhiteList("chitest01.test1").addBlackList("test.*");
////            Meta.DbInfo newDb = dbInfo.toBuilder().setTopicRules(0, topicBuilder).build();
////            client.setData().forPath(ZK_PATH + File.separator + host, newDb.toByteArray());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        client.close();
//    }
//}
