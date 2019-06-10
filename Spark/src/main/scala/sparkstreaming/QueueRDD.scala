package sparkstreaming

import java.util.concurrent.ConcurrentLinkedDeque

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 测试过程中，可以通过使用streamingContext.queueStream(queueOfRDDs)来创建DStream，每一个推送到这个队列中的RDD，都会作为一个DStream处理
  */
object QueueRDD {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("QueueRDD")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // 创建 QueueInputDStream
        val rddQueue: mutable.SynchronizedQueue[RDD[Int]] = new mutable.SynchronizedQueue[RDD[Int]]()

        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue)

        // 处理队列中的 RDD 数据
        val mappedStream: DStream[(Int, Int)] = inputStream.map(x => (x % 10, 1))
        val reducedStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)

        // 打印结果
        reducedStream.print()

        // 启动计算
        ssc.start()

        // 创建 RDD
        for (i <- 1 to 30) {
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }
    }
}
