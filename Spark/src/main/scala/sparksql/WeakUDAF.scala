package sparksql

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object WeakUDAF extends UserDefinedAggregateFunction{
    // 聚合函数输入参数的数据类型
    override def inputSchema: StructType = StructType(StructField("inputColumn", LongType) :: Nil)

    // 聚合缓冲区中值的数据类型
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", LongType) :: Nil)

    // 返回值的数据类型
    override def dataType: DataType = DoubleType

    // 对于相同的输入是否一直返回相同的输出
    override def deterministic: Boolean = true

    // 初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        // 存入工资的总额
        buffer(0) = 0L
        // 存入工资的人数
        buffer(1) = 0L
    }

    // 相同 Executor 间的数据合并
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        if (!input.isNullAt(0)) {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1
        }
    }

    // 不同 Executor 间的数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("WeakUDAF")
            .getOrCreate()

        spark.udf.register("myAverage", WeakUDAF)

        val df: DataFrame = spark.read.json("")
        df.createOrReplaceTempView("employees")
        df.show()
        val result: DataFrame = spark.sql("Select myAverage(salary) as average_salary from employees")
        result.show()
        spark.stop()
    }
}
