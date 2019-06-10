package sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.HashMap;

/**
 * @Author: magicyoung
 * @Date: 2019/5/23 15:25
 * @Description: SparkStreaming + kafka Receiver
 */
public class SparkStreamingOnKafkaReceiver {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingOnKafkaReceiver");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        HashMap<String, Integer> topicConsumerConcurrency = new HashMap<String, Integer>();

        topicConsumerConcurrency.put("t001", 1);


    }
}
