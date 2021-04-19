import java.io.File
import java.nio.charset.CodingErrorAction
import java.sql.Timestamp
import java.time.LocalDateTime

import LogGenerator._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.broadcast.Broadcast

import scala.io.{Codec, Source}
import Utilities._
/**
  * Author: Noah D. Garner
  * Init Date: Nov 25, 2019
  * Program attaches severities to logs, allows user to pick time range of input log to view
  * and allows uer to choose if they want to see just errors/warns, or all data.
  * Assumes broadcast commonErrors has been filtered for letters only
  * Program filters out numbers using regex in mapToRecord function
  * Takes 2 files, a failing, and passing, and does a join on the two
  * TODO: RRT streaming
  */

object RDD_Join_ErrorsNotIn {

  //Structure for our logs were message is still semi unstructured
  case class Record(timestamp: Timestamp,
                    message: String,
                    code: Int)

  //String is the value from record in a topic from a RRT stream (kafka)
  //Can't believe you can do this but whatever, cool stuff
  //God function
  def mapToRecord(data: String,
                  errorMapBC: Broadcast[Map[String, Int]],
                  errorRecords: Boolean,
                  ignoreIds: Boolean):
  Record = {

    //use date regex function to parse a Timestamp
    val dateTime = structureDate(data)

    //use message regex function to parse structuredMessage from logs
    val semiStructuredMessage = structureMessage(data, ignoreIds)

    //Retrieve the Map from the Broadcast variable (At Partition on cluster)
    val errMap = errorMapBC.value

    //the lines we want displayed if user passes true
    val keyWords = List("error", "warning", "fail", "crash")

    //if we want only errors in records, otherwise, we create a record
    //This needs to be changed so it returns Option[None] from function return
    if (errorRecords && !keyWords.exists(semiStructuredMessage.toLowerCase.contains(_)))
      Record(dateTime, "ignore", errMap.getOrElse(semiStructuredMessage, "000000".toInt))
    else
      Record(dateTime, semiStructuredMessage, errMap.getOrElse(semiStructuredMessage, "000000".toInt))
  }

  //structureMessage
  def structureMessage(data: String, ignoreIds: Boolean):
  String = {

    //match and group anything (.*) after IDEWD, a space, any # of chars, a colon,
    //any number of white space (0-100)
    //val messageRegex = "((?<=[IDEWD]\\s\\w{0,100}:\\s{0,100})(.*))"

    //used to grab the data wanted from a line
    val messageRegex = "((?<=[IDEWD]\\D{0,100}?:)(.*))"

    //convert bool to string regex, this uses val, instead of using
    //var with an if statement
    def convert(bool: Boolean): String = bool match {
      case true => "session_id\\:\\s\\S*| pair\\sid\\:\\s\\S*| notification_id\\:\\s\\S*| event_id\\:\\s\\S*| action_queue\\:\\s\\S"
      case false => ""
    }

    //keeps it immutable by applying a match statement based on the bool
    val ignores = convert(ignoreIds)

    //grabs data we want replacing digits with empty space, they cause clutter
    //in our reductions, and return it
    messageRegex
      .r
      .findFirstIn(data)
      //data wasn't there, or did not conform to our regex
      .getOrElse("bad message")
      .replaceAll("\\d", "")
      //lookbehind 0-2 colons, if its a space, replace colon(s) with a space.
      .replaceAll("((?<=\\s)(\\:{0,2}))", " ")
      //lookbehind 0-2 colons, if its not a word, replace with "". Takes care of the [::, but keeps : between chars
      .replaceAll("(?<=\\W)(\\:{0,2})", "")
      //replace any string of spaces with 1 space
      .replaceAll("\\s{2,}", " ")
      //replace redundant id information with "", makes aggregation nicer
      .replaceAll(ignores, "")
  }

  def structureDate(data: String):
  Timestamp = {

    //what is the format of the date you're parsing? IS it between <>?
    val dateRegex = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{0,6})"

    val date = "2019-" + dateRegex
      .r
      .findFirstIn(data)
      //date wasn't there, or did not conform to our regex
      .getOrElse("bad date")
      .replace(" ", "T")

    //converts OSI8601 String to LocalTime -> timestamp
    Timestamp
      .valueOf(LocalDateTime.parse(date))
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
      .fromFile(s"${file}")
      .getLines

    //load the map with data in errorFile
    for (line <- errorFile)
      errorMap += (line.split(",")(0) -> line.split(",")(1).trim.toInt)

    //returns the map
    errorMap
  }

  def main(args: Array[String]): Unit = {

    //controls the logging output in intellij console, less verbose (ie not there)
    setupLogging
    //so we no longer have to manually delete generated file. Sweet.
    FileUtils.deleteDirectory(new File("src/main/resources/output"))

    //1. Download logcat and put in logs folder under resources.
    //2 Change afterTimestamp  reflecting crash section you want (grab from test-output.txt)
    //3 change errorRecordsOnly or ignoreIds to display only logs with error/warning/fail/crash and remove ids
    val errorRecordsOnly = true
    val ignoreIds = true
    val failedDevice = "CSFT209"
    val passedDevice = "CSFT193"
    //grab this from your test-output.txt
    val afterTimestamp = "12-19 00:46:47"

    //for checking the count dynamically in the sql where clause
    var count = "0"

    val failFile =
      s"""failing${failedDevice}.txt"""
    val passFile =
      s"""passing${passedDevice}.txt"""

    //passing file
    val passFileLocation =
      s"""src/main/resources/logs/${passFile}"""
    //failing file
    val failFileLocation =
      s"""src/main/resources/logs/${failFile}"""
    val saveFolder =
      s"""src/main/resources/output"""
    //contains error, code, devices it occurred on in a CSV
    val commonErrorsFile =
      s"""src/main/resources/broadcastfiles/ImportantErrors.txt"""
    val completeTime =
      s"""2019-${afterTimestamp}.%"""

    //oh, now I know why stripMArgin, for console output.
    println(
      s"""Beginning script to scan files
         |errorRecordsOnly: ${errorRecordsOnly}
         |ignoreIds: ${ignoreIds}
      """
        .stripMargin
    )

    //dataset api notice the config is part of the session start
    // and  utilize Spark's Dataset API with the implicit
    val spark = SparkSession
      .builder
      .appName("RDD_Join_ErrorsNotIn.scala")
      .master("local[8]")
      .getOrCreate
    import spark.implicits._

    //Doesn't do much for our RDD_Join_ErrorsNotIn script
    val errorMap = spark
      .sparkContext
      .broadcast(loadCommonErrors(commonErrorsFile))

    //DS API not advanced enough for timestamps. Use DF api
    //double where clause works, good for future.
    val passingDS = spark
      .sparkContext
      .textFile(s"${passFileLocation}")
      .map(line => mapToRecord(line, errorMap, errorRecordsOnly, ignoreIds))
      .toDS

    //DS API not advanced enough for timestamps. Use DF api
    //double where clause works, good for future.
    val failingDS = spark
      .sparkContext
      .textFile(s"${failFileLocation}")
      .map(line => mapToRecord(line, errorMap, errorRecordsOnly, ignoreIds))
      .toDS

    //get all the records after this point in time from failing file
    val failingDataAfterTimestamp = failingDS
      .select("*")

    //create temp views to query with sql
    passingDS
      .createOrReplaceTempView("passing")
    failingDataAfterTimestamp
      .createOrReplaceTempView("failing")

    //To show all elements in failing file after time
    println(s"""Num elements: ${failingDS.rdd.count}""")

    //dynamically add to the sql statement a having clause if the user wants to
    //see only records that appear a certain # of times
    //having can be used instead of a subselect in the where clause after group by
    if (count != "0") count = s"""HAVING count = ${count}"""
    else count = ""

    //keep all the records we found in the failing file that were not
    //in the passing file. ie. to see differences that may cause failure
    //ORDER BY count DESC to show some messages may always appear with other messages,
    //and to. Can use explain to find out where you should put indexes
    val joinedErrorsNotIn = spark
      .sql(
        s"""
            SELECT a.message, count(a.message) AS count, a.code
            FROM failing a
            LEFT JOIN passing b
            USING (message)
            WHERE b.message IS NULL AND message NOT IN ("ignore")
            GROUP BY a.message, a.code
            ${count}
            ORDER BY count ASC
          """
      )

    //Format will be [message, # occurrences, code]
    joinedErrorsNotIn
      .rdd
      .coalesce(1)
      .saveAsTextFile(s"${saveFolder}/message")

    //so we can cross analyze with timestamps, also used to debug whether the file was read in correctly
    failingDataAfterTimestamp
      .select("timestamp", "message")
      .rdd
      .coalesce(1)
      .saveAsTextFile(s"${saveFolder}/time_message")

    //dynamically extract the # elements in the DS and print to console to see difference
    println(s"Num elements: ${joinedErrorsNotIn.rdd.count}")
  }
}
