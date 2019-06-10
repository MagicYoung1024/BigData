package sparkstreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/22 14:08
 * @Description: 单词个数统计加强版
 */
public class UpdateStateByKeyTest {
    public static void main(String[] args) {
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

        JavaPairDStream<String, Integer> stringIntegerJavaPairDStream = pairWords.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            public Optional<Integer> call(List<Integer> integers, Optional<Integer> integerOptional) throws Exception {
                int count = 0;
                if (integerOptional.isPresent()) {
                    count = integerOptional.get();
                }
                for (int v : integers) {
                    count += v;
                }
                return Optional.of(count);
            }
        });
    }
}
