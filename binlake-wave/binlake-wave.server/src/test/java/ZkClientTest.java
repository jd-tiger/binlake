import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

public class ZkClientTest {
    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                "127.0.0.1:2181",
                new RetryNTimes(10, 1000)
        );

        client.start();

        String path = "/zk/wave3/";

        while (path.endsWith("/")) { // 去除 /
            path = path.substring(0, path.lastIndexOf("/"));
        }
        System.err.println("path " + path);

        if (client.checkExists().forPath(path) == null) {
            String prefix = "";
            for (String name : path.split("/")) {
                prefix = prefix + "/" + name;
                if (prefix.equals("/")) {
                    prefix = "";
                    continue; // 仅仅是根路径
                }

                if (client.checkExists().forPath(prefix) == null) {
                    client.create().forPath(prefix, prefix.getBytes());
                }
            }
        }
    }
}