import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.regex.Pattern
import LogGenerator._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.window
import Utilities._
/** Window with parsed timeStamp method implemented. Study the structure and
  * format of the windowing operation.
  * Takes least appearing log line, helps me narrow down the issue on Newman Nest Aware ard Reboot
  * utilizing NoneNone-newman-z31a-20191025-181529.txt in Work_Logs
  * WORKING WITH LOCALDATETIME: CLEAN UP THE MAP METHOD
  * Add a method that matches the log lines after it strips out the numbers
  * because the numbers will make what should be similar lines, unsimilar
  * See RDD_NewmanFilterSoakTest$.scala for reason why this occurs
  * This is very good because sql databases utilize ISO 8601 time format!!!
  * */

object SS_NewmanTCP_MessageCount {

  //structure for our log data
  case class Record(timestamp: Timestamp, message: String, charCount: Int)

  //Row is a Record from an RDD from a RRT stream
  def mapDeskLogs(data: Row, dateRegex: String): Record = {
    val stringData = data.getString(0)

    //okay lets work with regex to find Date **-** and the data we need
    val dateMatcher = Pattern.compile(dateRegex).matcher(stringData)
    val messageMatcher = Pattern.compile(("((?<=[I|D|E])(.*))")).matcher(stringData)

    //match the pattern on the stringData from data.getString(0)
    //Timestamp in format ISO 8601 meaning, prepend 2019- to conform
    if (dateMatcher.find && messageMatcher.find) {
      //dateMAtcher.find is necessary. dumb syntax
      val dateString = "2019-" + dateMatcher
        .group(1)
        .replace(" ", "T")
      //convert localDateTime to Timestamp, concerns millis
      val localDateTime = LocalDateTime
        .parse(dateString)

      //replace all the digits in data, they skew the message similarity
      val messageString = messageMatcher
        .group(1)
        .replaceAll("\\d", "")

      //creates a Record Object with Timestamp field, generalized message no #s, and message's length
      Record(Timestamp.valueOf(localDateTime), messageString, messageString.length)
    }
    //handle the case that our matcher didn't work out
    else Record(null, "bad message", 0)
  }

  def main(args: Array[String]): Unit = {

    //to be command line arguments, this for testing
    val PORT = 9999
    val HOSTNAME = "127.0.0.1"
    //what is the format of the date you're parsing? IS it between <>? This makes it easy
    //Ask for people to put data between these types of signals we'll call them
    val date = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3})"
    val dateRegexList =
      List(
        "\\<(.*)>",
        date
      )

    //set our log level to error, avoid terminal spam during processing
    setupLogging

    //create our session for SQL commands and DS API's
    val spark = SparkSession
      .builder
      .appName("SSOnTCPSocket")
      .master("local[8]")
      .getOrCreate

    //import spark implicits so we can use DS and DF and sql
    import spark.implicits._

    //create our stream, we need to set a checkpoint for when we sink to textFile
    val structuredStream = spark
      .readStream
      .format("socket")
      //Port 9998 in build config
      .option("host", HOSTNAME).option("port", PORT)
      .load

    //create objects out of the stream of data coming in, note this is a real stream, real log data
    val structuredWithStamp = structuredStream
      //flatmap since some rows will not make the cut
      .map(x => mapDeskLogs(x, dateRegexList(1)))
      //filterNot bad messages
      .filter(!_.message.equals("bad message"))

    //SQL to group by Timestamp window
    val countedMessagesByWindow = structuredWithStamp
      .select("timestamp", "message")
      .withWatermark("timestamp", "2 seconds")
      //aggregate by time, $ converts count to a column.
      .groupBy(window($"timestamp", "2 seconds"), $"message")
      .count
      //must refer to as column with $ to use .desc method
      .orderBy($"window".desc)
      //convert back to object that we can query for count != 1.
      .where("count > 1")

    //sink to the console uncomment above to see fileSink
    val consoleSink = countedMessagesByWindow
      .writeStream
      .outputMode("complete")
      .format("console")

    //sink to IDE console
    consoleSink
      .start
      .awaitTermination

    //close our SparkSession, prevent leaks
    spark
      .stop
  }
}
