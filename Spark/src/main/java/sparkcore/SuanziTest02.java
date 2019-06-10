package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * @Author: magicyoung
 * @Date: 2019/5/21 8:56
 * @Description:
 */
public class SuanziTest02 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparkcore.SuanziTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("zhangsan", "18"),
                new Tuple2<String, String>("zhangsan", "180"),
                new Tuple2<String, String>("lisi", "19"),
                new Tuple2<String, String>("lisi", "190"),
                new Tuple2<String, String>("wangwu", "100"),
                new Tuple2<String, String>("wangwu", "200")
        ));
        JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
                new Tuple2<String, String>("zhangsan", "100"),
                new Tuple2<String, String>("zhangsan", "200"),
                new Tuple2<String, String>("lisi", "300"),
                new Tuple2<String, String>("lisi", "400"),
                new Tuple2<String, String>("wangwu", "500"),
                new Tuple2<String, String>("wangwu", "600")
        ));

        /**
         * zip
         * 将两个RDD压缩成一个K，V格式的RDD
         * 两个RDD中每个分区中的数量要一致
         */
        JavaPairRDD<Tuple2<String, String>, Tuple2<String, String>> zip = rdd1.zip(rdd2);
        zip.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>() {
            public void call(Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple2Tuple2Tuple2) throws Exception {
                System.out.println(tuple2Tuple2Tuple2);
            }
        });

        /**
         * zipWithIndex
         */
        JavaPairRDD<Tuple2<String, String>, Long> tuple2LongJavaPairRDD = rdd1.zipWithIndex();

        /**
         * countByKey
         */
        Map<String, Long> stringLongMap = rdd1.countByKey();

        /**
         * countByValue
         */
        Map<Tuple2<String, String>, Long> tuple2LongMap = rdd1.countByValue();
        sc.stop();
    }
}
