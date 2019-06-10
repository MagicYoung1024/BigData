import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CacheTest {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("CacheTest")

        val sc: SparkContext = new SparkContext(conf)

        sc.setCheckpointDir("./checkpoint")

        val RDD1: RDD[String] = sc.textFile("F:\\file")

        RDD1.checkpoint()

        RDD1.foreach(println(_))
    }
}
