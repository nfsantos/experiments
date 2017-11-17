package raw.stream

import java.nio.file.{Files, Paths}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status


object CheckpointsTwitterStreaming extends TwitterCredentials {
  val checkpointDir = "/tmp/sparkstreaming"

  def createNew(): StreamingContext = {
    Console.println("Creating a new Streaming context")
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStream")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.remember(Seconds(30))
    val twitterStream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, storageLevel = StorageLevel.MEMORY_ONLY)

    var allTweets = ssc.sparkContext.emptyRDD[Status]

    twitterStream.checkpoint(Seconds(10))
    twitterStream.foreachRDD(thisRdd => {
      Console.println("Current RDD : " + thisRdd)
      allTweets = allTweets.union(thisRdd)
      allTweets.persist()
      Console.println("AllTweets: " + allTweets + ", Deps: " + allTweets.dependencies.mkString(", "))
      Console.println("Current RDD count: " + thisRdd.count())
      Console.println("History RDD    : " + allTweets.count())
    })

    // Prepare checkpoint
    Files.createDirectories(Paths.get(checkpointDir))
    ssc.checkpoint(checkpointDir)
    ssc
  }

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpointDir, createNew)
    ssc.start()
    ssc.awaitTermination()
  }
}
