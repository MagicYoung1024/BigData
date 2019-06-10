package kafka;

import kafka.serializer.StringEncoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * @Author: magicyoung
 * @Date: 2019/5/23 14:57
 * @Description: 向kafka中生产数据
 */
public class SparkStreamingDataManuallyProducerForKafka extends Thread {
    static String[] channelNames = new String[] {
        "Spark", "Scala", "Kafka", "Flink", "Hadoop", "Storm",
        "Hive", "Impala", "HBase", "ML"
    };

    static String[] actionNames = new String[]{"View", "Register"};

    private String topic;
    private KafkaProducer<String, String> producerForKafka;

    private static String dataToday;
    private static Random random;

    public SparkStreamingDataManuallyProducerForKafka(String topic) {
        dataToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic = topic;
        random = new Random();
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "Node01:9092,Node02:9092,Node03:9092");
        properties.put("key.serializer.class", StringEncoder.class.getName());
        properties.put("serializer.class", StringEncoder.class.getName());
        producerForKafka = new KafkaProducer<String, String>(properties);
    }

    public void run() {
        int counter = 0;
        while (true) {
            counter++;
            String userLog = userlogs();
            producerForKafka.send(new ProducerRecord<String, String>(topic, userLog));

            if (0 == counter % 2) {
                counter = 0;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new SparkStreamingDataManuallyProducerForKafka("t001").start();
    }

    private static String userlogs() {
        StringBuffer userLogBuffer = new StringBuffer("");
        int[] unregisteredUsers = {1, 2, 3, 4, 5, 6, 7, 8};
        long timestamp = new Date().getTime();
        Long userID = 0L;
        long pageID = 0L;

        if (unregisteredUsers[random.nextInt(8)] == 1) {
            userID = null;
        } else {
            userID = Long.valueOf(random.nextInt(2000));
        }

        pageID = random.nextInt(2000);

        String channel = channelNames[random.nextInt(10)];

        String action = actionNames[random.nextInt(2)];

        userLogBuffer.append(dataToday).append("\t").append(timestamp).append("\t").append(userID).append("\t").append(pageID).append("\t").append(channel).append("\t").append(action);

        System.out.println(userLogBuffer.toString());
        return userLogBuffer.toString();
    }
}
