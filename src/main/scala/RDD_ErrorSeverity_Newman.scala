import java.nio.charset.CodingErrorAction
import java.sql.Timestamp
import java.time.LocalDateTime
import LogGenerator._
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast
import scala.io.{Codec, Source}
import Utilities._

/**
  * Program attaches severities to logs, allows user to pick time range of input log to view
  * and allows uer to choose if they want to see just errors/warns, or all data.
  * Assumes broadcast commonErrors has been filtered for letters only
  * Program filters out numbers using regex in mapToRecord function
  * TODO: RRT streaming
  */

object RDD_ErrorSeverity_Newman {

  //Structure for our logs were message is still semi unstructured
  case class Record(timestamp: Timestamp,
                    message: String,
                    severity: Int)

  //String is the value from record in a topic from a RRT stream (kafka)
  //Can't believe you can do this but whatever, cool stuff
  def mapToRecord(data: String,
                  dateRegex: String,
                  messageRegex: String,
                  errorMapBC: Broadcast[Map[String, Int]],
                  errorRecords: Boolean):
  Record = {

    val date = "2019-" + dateRegex
      .r
      .findFirstIn(data)
      .getOrElse("bad date")
      .replace(" ", "T")

    //converts OSI8601 String to LocalTime -> timestamp
    val dateTime = Timestamp
      .valueOf(LocalDateTime.parse(date))

    //grabs data we want replacing digits with empty space, they cause clutter
    //in our reductions
    val messageString = messageRegex
      .r
      .findFirstIn(data)
      .getOrElse("bad message")
      .replaceAll("\\d", "")
      .trim

    //thank you documentation, take the Map out of Broadcast while on node
    val errMap = errorMapBC.value

    //if we want only errors in records, otherwise, we create a record, why do none of them contain error? wtf
    if (errorRecords && !(messageString.toLowerCase.contains("error") || messageString.toLowerCase.contains("warning"))) {
      Record(dateTime, "ignore", -1)
    }
    else
      Record(dateTime, messageString, errMap.getOrElse(messageString, 0))
  }

  //loads known errors into a map that will be broadcasted for use on nodes
  def loadCommonErrors(file: String):

  Map[String, Int] = {

    // Handle character encoding issues for when on cluster: (woops)
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create empty map to fill
    var errorMap: Map[String, Int] = Map()
    //loadFile into collection of lines
    val errorFile = Source
      .fromFile(s"$file")
      .getLines
    //load the map with data in errorFile
    for (line <- errorFile) {
      val parsedLine = line.split(",")
      val error = parsedLine(0)
      val severity = parsedLine(1).trim.toInt
      errorMap += (error -> severity)
    }
    //returns the map
    errorMap
  }

  def main(args: Array[String]): Unit = {

    setupLogging

    //1. Download logcat and put in logs folder under resources. Change time Ranges reflecting crash
    //2. adjust severity level based on which errors you want to look for that have occurred in past
    //3. change errorRecords to true if you want to see only errors/warnings in the given time frame, not info
    val severityLevel = 0
    val errorRecords = false
    val rangeStart = """2019-12-03 17:10:32.%"""
    val rangeEnd = """2019-12-03 17:10:42.%"""
    val logcatLogs = """src/main/resources/logs/failingCSFT203.txt"""
    val saveFile = """src/main/resources/output"""
    val commonErrorsFile = """src/main/resources/broadcastfiles/ImportantErrors.txt"""

    println(s"Beginning script to scan file at begin time: $rangeStart to $rangeEnd.")

    //what is the format of the date you're parsing? IS it between <>?
    val dateRegex = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{0,6})"

    //match and group anything (.*) after IDEWD, a space, any # of chars, a colon,
    //any number of white space (0-100)
    val messageRegex = "((?<=[IDEWD]\\s\\w{0,100}:\\s{0,100})(.*))"

    //dataset api notice the config is part of the session start
    val spark = SparkSession
      .builder
      .appName("RDD_ErrorSeverity_Newman.scala")
      .master("local[8]")
      .getOrCreate
    //to utilize datasets
    import spark.implicits._

    val errorMap = spark
      .sparkContext
      .broadcast(loadCommonErrors(commonErrorsFile))

    //DS API not advanced enough for timestamps. Use DF api
    //double where clause works, good for future.
    val logsDF = spark
      .sparkContext
      .textFile(s"$logcatLogs")
      .map(line => mapToRecord(line, dateRegex, messageRegex, errorMap, errorRecords))
      .toDS

    //use this for seeing if in this time range, we have any errors we've seen before
    val logsInTimeRange = logsDF
      .select("*")
      .where($"severity" >= severityLevel)
      //uncomment for specific time range in logs to display
      //.where(s"cast(timestamp as String) between '$rangeStart' and '${rangeEnd}")

    logsInTimeRange
      .select("timestamp", "message")
      .where(s"cast(timestamp as String) between '${rangeStart}' and '${rangeEnd}'")
      .select("message")
      .groupBy("message")
      .count
      .where("count < 10 AND count > 1")
      .orderBy($"count")
      .rdd
      .coalesce(1)
      .saveAsTextFile(s"${saveFile}/message")

    logsInTimeRange
      .select("timestamp", "message")
      .where(s"cast(timestamp as String) between '${rangeStart}' and '${rangeEnd}'")
      .rdd
      .coalesce(1)
      .saveAsTextFile(s"${saveFile}/timeMessage")

    println(s"Num elements: ${logsInTimeRange.rdd.count}")

  }
}
