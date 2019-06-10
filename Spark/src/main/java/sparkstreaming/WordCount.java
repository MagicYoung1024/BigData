package sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: magicyoung
 * @Date: 2019/5/22 10:46
 * @Description: 流式统计单词总数
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));

        JavaReceiverInputDStream<String> socketTextStream = jsc.socketTextStream("Node01", 9999);

        JavaDStream<String> words = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String lines) throws Exception {
                return Arrays.asList(lines.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> reduceByKey = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        reduceByKey.print();

        reduceByKey.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                SparkContext context = rdd.context();
                JavaSparkContext sparkContext = new JavaSparkContext(context);
                Broadcast<String> broadcast = sparkContext.broadcast("");
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.stop();
    }
}
