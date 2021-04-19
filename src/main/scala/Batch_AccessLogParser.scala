import Utilities._
import java.util.regex.Matcher

import SparkHelper._
import LogGenerator._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Maintains top URL's visited over a 5 minute window, from a stream
  * of Apache access logs on port 9999.
  */
object Batch_AccessLogParser {

  def main(args: Array[String]) {

    val PORT = 9999
    val HOSTNAME = "127.0.0.1"

    setupLogging

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream(HOSTNAME, PORT, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line (field 5, doesnt start 0 idex, starts 1)
    val requests = lines
      .map(x => {
        val matcher: Matcher = pattern.matcher(x)
        if (matcher.matches) matcher.group(5)
      })

    val easyRequests = lines
      .map(_.split(" ")(2))


    easyRequests
      .print

    // Extract the URL from the request
    val urls = easyRequests
      .map(x => {
        val arr = x.split(" ");
        if (arr.size == 3)
          arr(1) else ""
      })
      .filter(!_.equals(""))

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = easyRequests
      .map(x => (x, 1))
      .reduceByKeyAndWindow(
        _ + _,
        _ - _,
        Seconds(10),
        Seconds(1))

    // Sort and print the results / now most prevalent error message
    val sortedResults = urlCounts
      .transform(_.sortBy(_._2, false))

    sortedResults
      .print

    //only use this when saving RDD data to DB or textFile
    sortedResults.foreachRDD((rdd,time) => {

      val repartitionedRDD = rdd.repartition(1).cache
      //only save if
      if (repartitionedRDD.count > 0) {
        repartitionedRDD
          .map(_._1)
          .saveAsSingleTextFile("src/main/resources/RDDFiles/logdata"+time.milliseconds.toString)
      }
    })

    // Kick it off
    ssc
      .checkpoint("src/main/resources/RDDFiles/checkpointdir")
    ssc
      .start
    ssc
      .awaitTermination
  }
}

