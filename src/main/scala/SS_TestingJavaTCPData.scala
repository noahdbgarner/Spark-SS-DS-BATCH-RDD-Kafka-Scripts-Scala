import org.apache.spark.sql._
import org.apache.spark.sql.functions.window

import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.regex.Pattern

/** Converts randomdData gen by Java Method, Maps to sentences occurances > to <
  * Simply run [ncat -lk [localhost] [PORT] | ncat -lk [localhost] [PORT]
  * Then run the java program with delay to generate random data w/ port tunneling
  * Then run Spark Script and tune script to extract/clean data you need
  * NOTE THIS DOES NOT USE KAFKA*/

object SS_TestingJavaTCPData {

  final case class Record(timestamp: Timestamp, code: Int, text: String)

  //serious drunk coding function. Go over this sober lmfao. Needs to be debugged.
  def mapToDateTime(data: Row): Record = {
    //get row as a string
    val stringData = data.getString(0)
    //first get the data we need
    val str = stringData.split(" ")
    //keep only data after the 6th element (our Some,None, Except Handler)
    val myData = Some(str.drop(2).mkString(" ")).getOrElse("empty")
    //This means it wasn't a valid timeStamp+data log line
    if (myData.equals("empty")) {
      return Record(null,0, "empty")
    }

    // The () parens create a group, used below in conditional
    val datePatternNewmanLog = Pattern.compile("(.*)")
    //match the pattern on the stringData from data.getString(0)
    val dateMatcher = datePatternNewmanLog.matcher(stringData)
    if (dateMatcher.find) {
      //grabs the first match, xform to ISO 8601 meaning first group between <> ISO 86 Timestamp
      val dateString = dateMatcher
        .group(1).replace(" ", "T")
      //Exact timeStamp matches will have 26 characters in this case
      if (dateString.length != 26) {
        return Record(null,0, "empty")
      }
      val dateTime = LocalDateTime.parse(dateString)
      //this converts LocalDateTime to a Timestamp
      //Needed LocalDateTime to parse nanoseconds. Then convert to TS. BOOM
      Record(Timestamp.valueOf(dateTime), str(2).toInt, myData)
    }
    else
    //Did not find a date on this line, check for empty in first field later
      Record(null,0, "empty")
  }

  def main(args: Array[String]): Unit = {

    //port, host
    val PORT = 9998
    val HOSTNAME = "127.0.0.1"

    //only want error spam
    Utilities.setupLogging

    System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    //set up sparkSession for structured streams and Spark SQL
    val spark = SparkSession
      .builder
      .appName("TestingJavaTCPData")
      .master("local[8]")
      .getOrCreate

    //import implicits so we can work with Datasets
    import spark.implicits._

    //set up our spark for sql queries, only working on this IP for some reason
    val structuredStream = spark
      .readStream
      .format("socket")
      .option("host", HOSTNAME)
      .option("port", PORT)
      .load

    //map the data into objects
    val structuredObjects = structuredStream
      //stamps data with event time as its created
      .map(mapToDateTime)
      //filter records that contain empty data from the mapper
      .filter(_.timestamp != null)

    //perform our query, there must be an aggregation to perform a writeStream op
    val allData = structuredObjects
      .select("timestamp", "text")
      //FIXED! Just needed to convert localDatetime to timestamp! See mapper
      .withWatermark("timestamp", "1 second")
      .groupBy(window($"timestamp", "1 second"), $"text")
      .count
      //only the first window will be display if this is [$"".asc]
      .orderBy($"window".desc)

    //write the stream
    val query = allData
      .writeStream
      .outputMode("complete")
      .format("console")

    //begin the data output
    query
      .start
      .awaitTermination

    //close connection
    spark
      .stop
  }
}
