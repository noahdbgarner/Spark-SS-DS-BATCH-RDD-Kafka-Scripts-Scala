import SparkHelper._
import LogGenerator._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
/**
  *
  */

object Batch_NewmanCountError {

  def filter(line: String): Boolean = {
    //split the string into an array of only words
    val parsedLine = line.toLowerCase.split("\\W")
    //keep lines of certain length, or if they contain error or warning, not info
    line.length < 210 &&
      (parsedLine.contains("error")
        || parsedLine.contains("restarting")
        || parsedLine.contains("warning")) &&
      !parsedLine.contains("info")
  }

  //for normal check, the numbers are based on columns in log files
  def mapper(line: String): String = {
    var msgType = ""
    if (line.toLowerCase.contains("warning")) msgType = "WARNING "
    if (line.toLowerCase.contains("error")) msgType = "ERROR "
      msgType+line
      .split("\\s+")
      .drop(10)
        //make array a string with a space between everything
      .mkString(" ")
  }

  def main(args: Array[String]): Unit = {

    //assign port value
    val PORT = 9999
    val HOSTNAME = "127.0.0.1"

    setupLogging
    //context with 1 second batches
    val ssc = new StreamingContext("local[8]","NewmanStreamedLogs", Seconds(1))

    //note we must use SparkSession with .readStream for DataSet API (ie structured streaming)
    val lines = ssc
      .socketTextStream(HOSTNAME, PORT, StorageLevel.MEMORY_AND_DISK_SER)

    val easyRequests = lines
      .filter(filter)
      .map(mapper)
      .map((_,1))

    //working for one print, lets loop it

    val reducedToTotals = easyRequests
        .reduceByKeyAndWindow(
          _+_,
          _-_,
          Seconds(30),
          Seconds(3)
        )
      .cache

    //transform allows us to sort every individual RDD
    val orderedResults = reducedToTotals
        .transform(_.sortBy(_._2, false))

    //prints combined results over a 10 second window, every second
    orderedResults
        .print(20)

    //this will save many text files btw.. thats what timestamp is for...
    orderedResults.foreachRDD((rdd,time) => {
      //we need to gather the partitions of the RDD together
      val repartitionedRDD = rdd.repartition(1).cache
      //our textFile saver only works with RDD of type RDD[String], get text
      repartitionedRDD
        .map(_._1)
        .saveAsSingleTextFile("src/main/resources/logFiles/outFiles/newman/nLogs"+time.milliseconds.toString)
    })

    //create recover points incase of fault
    ssc
      .checkpoint("src/main/resources/RDDFiles/checkpoints.txt")
    ssc
      .start
    ssc
      .awaitTermination

  }
}
