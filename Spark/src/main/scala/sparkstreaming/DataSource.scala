package sparkstreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DataSource {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("DataSource")
        val sc = new SparkContext(conf)

        val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

        val lines: DStream[String] = ssc.textFileStream("hdfs://Node01:9000//data/")

        val words: DStream[String] = lines.flatMap(_.split(" "))

        val pairWord: DStream[(String, Int)] = words.map((_, 1))

        val wordCounts: DStream[(String, Int)] = pairWord.reduceByKey(_ + _)

        wordCounts.print()

        ssc.start()
    }
}
