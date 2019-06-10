package sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDCreate {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("RDDCreate")
        val sc = new SparkContext(conf)

        val prdd = sc.parallelize(1 to 10)

        prdd.foreach(println(_))

        val lrdd = sc.parallelize(List("a", "b", "c"))

        lrdd.foreach(println(_))

        val mrdd = sc.makeRDD(0 to 10)

        mrdd.foreach(println(_))

        sc.stop()
    }
}
