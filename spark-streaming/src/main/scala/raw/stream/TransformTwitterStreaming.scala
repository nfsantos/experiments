package raw.stream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status


object TransformTwitterStreaming extends TwitterCredentials {

  def main(args: Array[String]): Unit = {
    Console.println("Creating a new Streaming context")
    val conf = new SparkConf().setMaster("local[2]").setAppName("TwitterStream")
    val ssc = new StreamingContext(conf, Seconds(5))
    val twitterStream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, storageLevel = StorageLevel.MEMORY_ONLY)

    var triggerWords = ssc.sparkContext.parallelize(Seq("house", "sea", "mountain"))
    // Use transform to create a new DStream using arbitrary RDD operations which are not defined on DStream.
    // TODO: Find a good use case
    val newDStream = twitterStream.transform { rdd =>
      rdd.flatMap(s => s.getText.split(raw"\s+")).intersection(triggerWords)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}