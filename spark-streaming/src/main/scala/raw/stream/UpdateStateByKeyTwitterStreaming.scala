package raw.stream

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status


object UpdateStateByKeyTwitterStreaming extends TwitterCredentials {

  def updateFunc(tweets: Seq[Status], value: Option[Int]): Option[Int] = {
    Some(value.getOrElse(0) + tweets.length)
  }

  val checkpointDir = "/tmp/sparkstreaming"

  def createNew(): StreamingContext = {
    Console.println("Creating a new Streaming context")
    val conf = new SparkConf().setMaster("local[4]").setAppName("TwitterStream")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create DStreams
    val filters = Seq.empty[String]
    var twitterAuth = None
    val twitterStream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth, filters, StorageLevel.MEMORY_ONLY)
    val keyByLang: DStream[(String, Status)] = twitterStream.map(s => (s.getLang, s))

    // Keeps persistent state per key.
    val langCount: DStream[(String, Int)] = keyByLang.updateStateByKey(updateFunc)

    langCount.foreachRDD(rdd => {
      val top10 = rdd.top(10)(Ordering.by({ case (lang, count) => count }))
      Console.println("top 10\n" + top10.mkString("\n"))
    })
    // Prepare checkpoint
    Files.createDirectories(Paths.get(checkpointDir))
    ssc.checkpoint(checkpointDir)
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, createNew)
    //    twitterStream.foreachRDD(rdd => {
    //      val keyByLang: RDD[(String, Status)] = rdd.keyBy(_.getLang)
    //      Console.println("Count: " + ptLang.count())
    //      Console.println("Sample: " + ptLang.take(10).map(s => s.getText).mkString("\n"))
    //    })

    ssc.start()
    ssc.awaitTermination()
  }
}