import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SuanziTest00 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("SuanziTest00")
        val sc = new SparkContext(conf)

        val rdd1 = sc.parallelize(Array("love1", "love2", "love3", "love4", "love5", "love6", "love7", "love8", "love9", "love10", "love11", "love12"), 2)
        val rdd2 = rdd1.mapPartitionsWithIndex((index, iter) => {
            val list = new ListBuffer[String]()
            while (iter.hasNext) {
                val tuple = iter.next()
                list.+=("rdd1 partition index = " + index + ", value = " + tuple)
            }
            list.iterator
        }, true)
        rdd2.foreach(println(_))

        sc.stop()
    }
}
