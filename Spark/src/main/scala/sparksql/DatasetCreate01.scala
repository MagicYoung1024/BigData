package sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object DatasetCreate01 {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
        .builder()
        .master("local")
        .appName("StructType")
        .getOrCreate()


        val peopleRDD: RDD[String] = spark.sparkContext.textFile("F:\\person.txt")

        val schemaString: String = "id name age"

        val fields: Array[StructField] = schemaString.split(" ")
            .map(fieldName => StructField(fieldName, StringType, nullable = true))
        val schema: StructType = StructType(fields)

        val rowRDD: RDD[Row] = peopleRDD
            .map(_.split(","))
            .map(attributes => Row(attributes(0), attributes(1), attributes(2).trim))

        val peopleDF: DataFrame = spark.createDataFrame(rowRDD, schema)

        peopleDF.createOrReplaceTempView("people")

        val results: DataFrame = spark.sql("select * from people")

        results.show()

        spark.stop()
    }
}
