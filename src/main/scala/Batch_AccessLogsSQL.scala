import Utilities._
import LogGenerator._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Illustrates using SparkSQL with Spark Streaming, to issue queries on 
  * Apache log data extracted from a stream on port 9999.
  * File Format for reading in TCP:
  */
object Batch_AccessLogsSQL {

  case class Record(url: String, status: Int, agent: String)

  // Extract the (URL, status, user agent) we want from each log line
  def extractData(line: String): (String, Int, String) = {
    val parsedLine = line.split(" ")
    //single quotes is a literal def, double quotes the val is interpreted
    val url = Some(parsedLine(6))
      .getOrElse("error")
      //take off the "
      .split('"')
      //convert the array returned by split back to a string
      .mkString
      //default error if bad data

    //default error if bad data
    val agent = Some(line
      //since the last data entry per line is the agent, split on quotes, get last
      .split('"').last)
      //notice the different .getOrElse params.. just like getOrDefault w/ java hMap
      .getOrElse("error")

    //what the fuck was i doing here
    val pattern = "[0-6][0-9][0-9]".r
    val status = pattern
      //drop 3 to skip IP, we know next 3 digits will be error code
      .findFirstIn(parsedLine.drop(3).mkString)
      //default 0 if bad data
      .getOrElse(0)
      //only way we could work with this
      .toString
      .toInt
    (url, status, agent)
  }

  def main(args: Array[String]) {

    val PORT = 9999
    val HOSTNAME = "127.0.0.1"

    //set log level error
    setupLogging

    //required for streaming API such as socketTextStream or TwitterUtils.createStream etc
    val ssc = new StreamingContext("local[8]", "LogSQL", Seconds(1))

    //required for working with SparkSQL anything, operates on 1 RDD at a time, see foreachRDD
    val spark = SparkSession
      .builder
      .master("local[8]")
      .getOrCreate

    //so we can use .toDS or .toDF, most importantly with .as[Business] type things
    import spark.implicits._

    // Create a socket stream to read log data published via netcat on port 9998 locally
    val lines = ssc
      .socketTextStream(HOSTNAME, PORT, StorageLevel.MEMORY_AND_DISK_SER)

    val requests = lines
      .map(extractData)

    // Process each RDD from each batch as it comes in
    // Note that we can control this output with a byWindow operation
    //operation before this foreachRDD call.
    //This is how we access each RDD to use Dataframe API, otherwise we
    //cannot do this
    requests
      .foreachRDD((rdd, time) => {

        // objects, which in turn we can convert to a DataSet using toDS()
        val requestsDataFrame = rdd
          .map(w => Record(w._1, w._2, w._3))
          .toDS

        // Create a SQL table from the DS
        requestsDataFrame
          .createOrReplaceTempView("requests")

        // Count up occurrences of each user agent in this RDD and print the results.
        // The powerful thing is that you can do any SQL you want here!
        // But remember it's only querying the data in this RDD, from this batch.
        val wordCountsDataFrame = spark
          .sql("select agent, count(*) as total from requests group by agent")

        println(s"========= $time =========")
        wordCountsDataFrame
          .show

        // If you want to dump data into an external database instead, check out the
        // org.apache.spark.sql.DataFrameWriter class! It can write dataframes via
        // jdbc and many other formats! You can use the "append" save mode to keep
        // adding data from each batch.
      })

    ssc
      .checkpoint("src/main/resources/RDDFiles/checkpoints/files")
    //start the stream loop
    ssc
      .start

    //wait for ctrl+c or pause to termination
    ssc
      .awaitTermination

    //close the SparkSession, just like closing DB connection
    spark
      .stop
  }
}


