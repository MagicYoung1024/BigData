package sparksql

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)

object StrongUDAF extends Aggregator[Employee, Average, Double] {
    // 定义一个数据结构，保存工资总数和人数，初始都是0
    def zero: Average = Average(0L, 0L)

    // 聚合同一个 Executor 的结果
    def reduce(buffer: Average, employee: Employee): Average = {
        buffer.sum = buffer.sum + employee.salary
        buffer.count += 1
        buffer
    }

    // 聚合不同 Execute 间的结果
    def merge(b1: Average, b2: Average): Average = {
        b1.sum += b2.sum
        b1.count += b2.count
        b1
    }

    // 计算输出
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // 设定之间值类型的编码器，要转换成case类
    // Encoders.product是进行scala元组和case类转换的编码器
    def bufferEncoder: Encoder[Average] = Encoders.product

    // 设定最终输出值的编码器
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("StrongUDAF")
            .getOrCreate()

        import spark.implicits._
        val ds: Dataset[Employee] = spark.read.json("").as[Employee]
        ds.show()

        val averageSalary: TypedColumn[Employee, Double] = StrongUDAF.toColumn.name("average_salary")
        val result: Dataset[Double] = ds.select(averageSalary)
        result.show()
        spark.stop()
    }
}
