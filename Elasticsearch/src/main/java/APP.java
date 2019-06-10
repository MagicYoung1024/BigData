import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @Author: magicyoung
 * @Date: 2019/5/29 15:51
 * @Description:
 */
public class APP {
    PreBuiltTransportClient client;

    /**
     * 获取Transport Client
     * （1）ElasticSearch服务默认端口9300。
     * （2）Web管理平台端口9200。
     * @throws UnknownHostException
     */
    @Before
    public void getClient() throws UnknownHostException {
        // 设置连接的集群名称
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();

        // 连接集群
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new TransportAddress(InetAddress.getByName("Node01"), 9300));

        // 打印集群名称
        System.out.println(client.toString());
    }

    /**
     * 创建索引
     */
    @Test
    public void createIndex() {
        // 创建索引
        client.admin().indices().prepareCreate("blog").get();

        // 关闭连接
        client.close();
    }

    /**
     * 删除索引
     */
    @Test
    public void deleteIndex() {
        // 删除索引
        client.admin().indices().prepareDelete("index").get();

        // 关闭连接
        client.close();
    }
}
