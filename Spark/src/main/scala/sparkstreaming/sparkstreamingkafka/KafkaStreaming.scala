package sparkstreaming.sparkstreamingkafka

import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object createKafkaProducerPool {
    def apply(brokerList: String, topic: String) : GenericObjectPool[KafkaProducerProxy] = {
        val producerFactory = new BaseKafkaProducerFactory(brokerList, defaultTopic = Option(topic))
        val pooledProducerAppFactory = new PooledKafkaProducerAppFactory(producerFactory)
        val poolConfig = {
            val c = new GenericObjectPoolConfig[KafkaProducerProxy]
            val maxNumProducers = 10
            c.setMaxTotal(maxNumProducers)
            c.setMaxIdle(maxNumProducers)
            c
        }
        new GenericObjectPool[KafkaProducerProxy](pooledProducerAppFactory, poolConfig)
    }
}

object KafkaStreaming {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[4]")
            .setAppName("Network WordCount")
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))

        // 创建 Topic
        val brobrokers = "Node01:9092,Node02:9092,Node03:9092"
        val sourceTopic = "source"
        val targetTopic = "target"

        // 创建消费者组
        val group = "con-consumer-group"

        // 消费者配置
        val kafkaParam = Map(
            "bootstrap.servers" -> brobrokers,  //用于初始化链接到集群的地址
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> group,    //用于标识这个消费者属于哪个消费团体
            //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
            //可以使用这个配置，latest自动重置偏移量为最新的偏移量
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)  //如果是true，则这个消费者的偏移量会在后台自动提交

        )

        //创建DStream，返回接收到的输入数据
        //var stream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))
        KafkaUtils.createDirectStream(ssc, Location)
    }
}
