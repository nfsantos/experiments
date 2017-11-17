package raw.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status


object WindowFunctionsTwitterStreaming extends TwitterCredentials {
  def main(args: Array[String]): Unit = {
    Console.println("Creating a new Streaming context")
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStream")
    val ssc = new StreamingContext(conf, Seconds(5))
    val twitterStream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, storageLevel = StorageLevel.MEMORY_ONLY)

    val byLangCount: DStream[(String, Int)] = twitterStream.map(s => (s.getLang, 1)).reduceByKey(_ + _)
    byLangCount.foreachRDD(rdd => {
      Console.println("Last batch count: " + rdd.collect().sortBy(f => f._1).mkString("\n"))
    })

    val lastWindow = byLangCount.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(20), Seconds(5))
    lastWindow.foreachRDD(rdd => {
      Console.println("Last 20 seconds count: " + rdd.collect().sortBy(f => f._1).mkString("\n"))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}