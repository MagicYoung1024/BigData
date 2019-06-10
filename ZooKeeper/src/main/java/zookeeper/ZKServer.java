package zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Author: magicyoung
 * @Date: 2019/4/19 15:56
 * @Description:
 */
public class ZKServer {
    private String connectString = "Node1:2181,Node2:2181,Node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkCli = null;
    private String parentNode = "/servers";

    public void getConnect() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    public void registerNode() throws KeeperException, InterruptedException {
        String hostname = "Node1";
        String path = zkCli.create(parentNode + "/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(path);
    }


    public void business(String hostname) throws InterruptedException {
        System.out.println(hostname + " is online!");
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        args = new String[]{"Node1"};

        ZKServer zkServer = new ZKServer();

        // 1. 获取zkServer连接
        zkServer.getConnect();

        // 2. 注册服务器节点
        zkServer.registerNode();

        // 3. 业务处理
        zkServer.business(args[0]);
    }
}
