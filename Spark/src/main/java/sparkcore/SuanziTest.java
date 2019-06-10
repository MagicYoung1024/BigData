package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/15 15:49
 * @Description:
 */
public class SuanziTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("sparkcore.SuanziTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Tuple2<String, Integer>> rdd1 = sc.parallelize(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 21),
                new Tuple2<String, Integer>("lisi", 18),
                new Tuple2<String, Integer>("wangwu", 19),
                new Tuple2<String, Integer>("zhaoliu", 20),
                new Tuple2<String, Integer>("maqi", 21)
        ));
        JavaRDD<Tuple2<String, Integer>> rdd2 = sc.parallelize(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 21),
                new Tuple2<String, Integer>("lisi", 18),
                new Tuple2<String, Integer>("wangwu", 19),
                new Tuple2<String, Integer>("zhaoliu", 20),
                new Tuple2<String, Integer>("xiaoming", 17)
        ));

        /**
         * 取交集
         */
        JavaRDD<Tuple2<String, Integer>> r1 = rdd1.intersection(rdd2);
        r1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> result) throws Exception {
                System.out.println(result);
            }
        });
        System.out.println("********************************");

        JavaRDD<Tuple2<String, Integer>> r2 = rdd1.union(rdd2);
        r2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> result) throws Exception {
                System.out.println(result);
            }
        });

        sc.stop();
    }
}
