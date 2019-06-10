import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationTest {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
        conf.setMaster("local").setAppName("test")

        val sc: SparkContext = new SparkContext(conf)
        val lines: RDD[String] = sc.textFile("F:\\file")
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val samples: RDD[String] = words.sample(true, 0.1)

        samples.foreach(println(_))

        sc.stop()
    }
}
