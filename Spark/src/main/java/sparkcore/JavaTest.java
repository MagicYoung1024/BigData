package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @Author: magicyoung
 * @Date: 2019/5/14 20:01
 * @Description:
 */
public class JavaTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("sparkcore.JavaTest");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> RDD1 = sc.textFile("F:\\file");

        JavaRDD<String> RDD2 = RDD1.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> RDD3 = RDD2.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> RDD4 = RDD3.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        RDD4.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> result) throws Exception {
                System.out.println(result);
            }
        });

        sc.stop();
    }
}
