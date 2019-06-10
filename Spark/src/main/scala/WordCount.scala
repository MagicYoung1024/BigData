import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
        conf.setMaster("local").setAppName("WordCount")

        val sc: SparkContext = new SparkContext(conf)
        val counts: RDD[(String, Int)] = sc.textFile("F:\\file").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
        counts.foreach(println(_))
    }
}
