package raw.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}


object FileMonitor {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Does not need a thread.
    val files: DStream[String] = ssc.textFileStream("/tmp/a")

    files.map(s => {
      Console.println("Read: " + s)
      1
    })

      .print()


    ssc.start()
    ssc.awaitTermination()
  }
}