package sparkstreaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import kafka.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

/**
  * 实现类，通过继承 Receiver，并实现 onStart 和 onStop 方法来自定义数据源采集
  * @param host
  * @param port
  */
class CustomReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

    override def onStart(): Unit = {
        // 开启一个线程接收数据
        new Thread("Socket Receiver") {
            override def run(): Unit = {
                receive()
            }
        }.start()
    }

    override def onStop(): Unit = {}

    // 创建 Socket 连接并接收传入的数据，直至结束
    def receive(): Unit = {
        var socket: Socket = null
        var userInput: String = null

        try {
            val socket: Socket = new Socket(host, port)
            val reader: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream, StandardCharsets.UTF_8))
            var userInput: String = reader.readLine()

            while (!isStopped() && userInput != null) {
                store(userInput)
                userInput = reader.readLine()
            }

            reader.close()
            socket.close()

            restart("Trying to connect again")
        } catch {
            case e: java.net.ConnectException =>
                restart("Error connect to " + host + ":" + port)
            case t: Throwable =>
                restart("Error receiving data", t)
        }
    }
}

/**
  * 通过自定义数据源，模拟 Spark 内置的 Socket 连接
  * 自定义数据源，实现 CustomReceiver 类来实现
  * 通过 StreamingContext.receiverStream(<instance of custom receiver>) 来使用自定义的数据采集源
  */
object CustomReceiver{
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf()
            .setMaster("local[2]")
            .setAppName("Network WordCount")

        val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
        val lines: ReceiverInputDStream[String] = ssc.receiverStream(new CustomReceiver("Node01", 9999))
        val words: DStream[String] = lines.flatMap(_.split(" "))
        val pairs: DStream[(String, Int)] = words.map((_, 1))
        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
        wordCounts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
