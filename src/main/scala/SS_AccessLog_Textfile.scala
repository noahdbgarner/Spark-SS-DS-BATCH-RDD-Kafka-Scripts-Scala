import Utilities._
import java.text.SimpleDateFormat
import java.util.Locale
import java.util.regex.{Matcher, Pattern}

import LogGenerator._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/** Uses SS and sends counts of messages to a textFile
  * Formmated for the Streaming course AccessLog File in /Logs Directory */

object SS_AccessLog_Textfile {

  // Case class defining structured data for a line of Apache access log data
  case class LogEntry(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)

  val logPattern = apacheLogPattern
  val datePattern = Pattern.compile("\\[(.*?) .+]")

  // Function to convert Apache log times to what Spark/SQL expects
  def parseDateField(field: String): Option[String] = {

    val dateMatcher = datePattern.matcher(field)
    if (dateMatcher.find) {
      val dateString = dateMatcher.group(1)
      //what if i format this simpledateformat
      val dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
      val date = (dateFormat.parse(dateString))
      val timestamp = new java.sql.Timestamp(date.getTime());
      return Option(timestamp.toString())
    } else {
      None
    }
  }

  // Convert a raw line of Apache access log data to a structured LogEntry object (or None if line is corrupt)
  def parseLog(x:Row) : Option[LogEntry] = {

    val matcher:Matcher = logPattern.matcher(x.getString(0));
    if (matcher.matches())
      return Some(LogEntry(
        matcher.group(1),
        matcher.group(2),
        matcher.group(3),
        parseDateField(matcher.group(4)).getOrElse(""),
        matcher.group(5),
        matcher.group(6),
        matcher.group(7),
        matcher.group(8),
        matcher.group(9)
      ))
     else return None
  }

  def main(args: Array[String]) {

    setupLogging
    // Notice we are not defining a batch time. Thats because data is processed
    //immediately, there is no wait. ITs considered real time
    val spark = SparkSession
      .builder
      .appName("StructuredStreaming")
      .master("local[*]")
      .getOrCreate

    // Must import spark.implicits for conversion to DataSet to work!
    import spark.implicits._

    // Create a stream of text files dumped into the logs directory
    val rawData = spark
      .readStream
      .text("src/main/resources/logs")

    //flatMap instead of Map because we flatMap will ignore null rows that are created
    //through the Some, None, Option pattern
    val structuredData = rawData
      .flatMap(parseLog)
      .select("status", "dateTime")

    // Group by status code, with a one-hour window.
    val windowed = structuredData
      //so this is our window for structured streams, okay this makes sense
      .groupBy($"status", window($"dateTime", "1 hour"))
      .count
      .orderBy("window")

    // Start the streaming query, dumping results to the console. Use "complete" output mode because we are aggregating
    // (instead of "append").
    val query = windowed
      .writeStream
      //can only be 3 different options, "update", append","complete"
      .outputMode("complete")
      //guess this can be a file?
      .format("console")
      .start

    // Keep going until we're stopped.
    query
      .awaitTermination

    spark
      .stop
  }
}
