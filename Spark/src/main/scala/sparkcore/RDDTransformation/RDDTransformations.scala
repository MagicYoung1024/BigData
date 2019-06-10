package sparkcore.RDDTransformation

import org.apache.spark.{SparkConf, SparkContext}

object RDDTransformations {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("RDDTransformations")
        val sc = new SparkContext(conf)

        val rdd0 = sc.parallelize(List("zhangsan", "lisi", "wangwu", "maliu", "wangwu", "wangwu", "zhangsan"))


    }
}
