import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Top5 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("Top5")
        val sc = new SparkContext(conf)

        val rdd1: RDD[String] = sc.textFile("F:\\file").map(_.split("\t")(4))
        val rdd2: RDD[(String, Int)] = rdd1.map((_, 1)).reduceByKey(_ + _)
        val rdd3: RDD[(String, Int)] = rdd2.sortBy(_._2, false)
        val rdd4: Array[(String, Int)] = rdd3.take(3)
        rdd4.foreach(println(_))
        sc.stop()
    }
}
