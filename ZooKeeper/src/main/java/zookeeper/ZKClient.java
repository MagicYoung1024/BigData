package zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/4/19 16:06
 * @Description:
 */
public class ZKClient {

    private String connectString = "Node1:2181,Node2:2181,Node3:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkCli = null;

    private void getConnect() throws IOException {
        zkCli = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                List<String> lists = new ArrayList<String>();
                try {
                    children = zkCli.getChildren("/servers", true);
                    for (String child : children) {
                        byte[] data = zkCli.getData("/servers/" + child, false, null);
                        lists.add(new String(data));
                    }
                    System.out.println(lists);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void getServerLists() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/servers", true);
        List<String> lists = new ArrayList<String>();

        for (String child : children) {
            byte[] data = zkCli.getData("/servers/" + child, false, null);
            lists.add(new String(data));
        }
        System.out.println(lists);
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZKClient zkCli = new ZKClient();

        // 1. 获取连接
        zkCli.getConnect();

        // 2. 监听服务器中节点路径
        zkCli.getServerLists();

        // 3. 业务处理
        zkCli.business();
    }
}
