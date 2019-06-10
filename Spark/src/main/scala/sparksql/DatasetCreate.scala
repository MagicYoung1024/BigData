package sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person1(id: Long, name: String, age: Long) extends Serializable

object DatasetCreate {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("Dataset")
            .getOrCreate()
        import spark.implicits._

        val peopleDF: DataFrame = spark.sparkContext
            .textFile("F:\\person.txt")
            .map(_.split(","))
            .map(attributes => Person1(attributes(0).trim.toLong, attributes(1), attributes(2).trim.toLong)).toDF()
        peopleDF.createOrReplaceTempView("people")

        val teenagersDF = spark.sql("select * from people where age > 18")

        teenagersDF.show()

        spark.stop()
    }
}
