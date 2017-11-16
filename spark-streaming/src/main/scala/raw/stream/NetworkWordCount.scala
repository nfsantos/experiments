package raw.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream


object NetworkWordCount {
  def main(args: Array[String]): Unit = {
    // Create a local StreamingContext with two working thread and batch interval of 1 second.
    // The master requires 2 cores to prevent from a starvation scenario.
    val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    // Allocates a thread to receive the data from the socket.
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    lines.repartition(2).foreachRDD(rdd => {
      Console.println(" " + rdd.id)
      rdd.foreachPartition(iter => Console.print(iter.mkString(", ")))
    })
    //    val words = lines.flatMap(_.split(" "))
    //    val pairs = words.map(w => (w, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}