package sparkstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        // 创建 SparkConf
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("WordCount")

        // 初始化 StreamingContext
        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

        // 定义消息输入源，创建 DStream
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("Node01", 9999)
        ssc.fileStream("")

        // DStream 转换操作
        val words: DStream[String] = lines.flatMap(_.split(" "))

        val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)

        // DStream 输出操作
        wordCounts.print()

        ssc.start()

        // 等待程序终止
        ssc.awaitTermination()
    }
}
