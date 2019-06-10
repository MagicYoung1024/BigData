package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/15 14:44
 * @Description:
 */
public class ParallelizeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparkcore.ParallelizeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> list = Arrays.asList("a", "b", "c", "d", "e", "f");
        JavaRDD<String> rdd = sc.parallelize(list);

        System.out.println("rdd paratition length = " + rdd.partitions().size());

        List<String> collect = rdd.collect();

        sc.stop();
    }
}
