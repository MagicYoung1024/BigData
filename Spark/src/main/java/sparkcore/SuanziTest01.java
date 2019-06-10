package sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author: magicyoung
 * @Date: 2019/5/20 19:39
 * @Description:
 */
public class SuanziTest01 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SuanziTest03");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList(
                "love1", "love2", "love3", "love4",
                "love5", "love6", "love7", "love8",
                "love9", "love10", "love11", "love12"
                ), 3);

        /**
         * mapPartitionWithIndex
         *
         */
        JavaRDD<String> mapPartitionsWithIndex = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (stringIterator.hasNext()) {
                    String one = stringIterator.next();
                    list.add("rdd1 partition = " + integer + ", value = " + one);
                }
                return list.iterator();
            }
        }, true);

        /**
         * repartition
         * 该算子是会产生shuffle的算子，可以对RDD重新分区
         * coalesce
         * 可以对RDD进行分区，可以指定是否进行shuffle，默认为false
         *
         * repartition = coalesce(numpartitions, true)
         */
        JavaRDD<String> rdd2 = mapPartitionsWithIndex.repartition(4);
        JavaRDD<String> stringJavaRDD = rdd2.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (stringIterator.hasNext()) {
                    String one = stringIterator.next();
                    list.add("rdd2 partition = " + integer + ", value = " + one);
                }
                return list.iterator();
            }
        }, true);

        List<String> collect = stringJavaRDD.collect();
        for(String s : collect) {
            System.out.println(s);
        }
//        System.out.println(rdd1.partitions().size());
        mapPartitionsWithIndex.collect();
        sc.stop();
    }
}
