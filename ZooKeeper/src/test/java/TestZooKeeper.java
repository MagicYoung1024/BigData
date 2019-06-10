import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/4/19 15:18
 * @Description: 测试
 */
public class TestZooKeeper {
    private String connectString = "Node1:2181,Node2:2181,Node3:2181";
    private int sessionTimeout = 2000;
    ZooKeeper zkClient = null;

    // 初始化ZK客户端
    @Before
    public void initClient() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
//                System.out.println(watchedEvent.getType() + "\t" + watchedEvent.getPath());
                /*System.out.println("-------------------start-----------------------");
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("-------------------end-----------------------");*/
            }
        });
    }

    // 创建子节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String path = zkClient.create("/magicyoung", "yang".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    // 获取子节点
    @Test
    public void getChild() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    // 判断znode是否存在
    @Test
    public void exist() throws KeeperException, InterruptedException {
        Stat exists = zkClient.exists("/magicyoung", false);
        System.out.println(exists == null ? "not exist" : "exist");
    }
}
