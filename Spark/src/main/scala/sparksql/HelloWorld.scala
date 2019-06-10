package sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object HelloWorld {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
        .builder()
        .master("local[2]")
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()

        import spark.implicits._

        val df: DataFrame = spark.read.json("F:\\json")

        df.show()

        df.filter($"age" > 21)

        df.filter(line => {
            val col = line.getAs[Long]("age")
            col > 19
        }).show()

        df.createOrReplaceTempView("person")

        spark.sql("select * from person where age >= 21").show()

        spark.stop()
    }
}
