import java.nio.charset.CodingErrorAction
import java.sql.Timestamp
import java.time.LocalDateTime
import org.apache.spark.sql.functions._
import LogGenerator._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.Trigger
import com.datastax.spark.connector.cql.CassandraConnectorConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.cassandra._
import scala.collection.mutable.ListBuffer
import scala.io.{Codec, Source}
import Utilities._
/**
  * This is necessary since the reasoning behind sensor fails could change, and
  * update over firmware versions, and as we learn more about failing sensors.
  * As long as I have updated Keeper File, I can immediately know what is
  * wrong with a sensor, or what I should do
  *
  */


object SS_Kafka_MetricsDB {

  final case class Record(timestamp: Timestamp,
                          message: String,
                          error: String,
                          occurrence: Int)

  //Creates a timestamp object if there is a timestamp in a string
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

  //(helper) loads known errors into a map that will be broadcasted for use on nodes
  def loadAlerts(file: String): Map[String, String] = {
    //Handle character encoding issues for when on a node in the cluster
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val messageRegex =
      "((?<=(TRUE|FALSE).*(TRUE|FALSE).*(TRUE|FALSE).*(TRUE|FALSE),))(.*)"

    //create empty map to fill
    var alertMap: Map[String, String] = Map()

    val lines = Source.fromFile(file).getLines

    //something wrong putting the string into the map here...
    for(line <- lines) {

      val message = messageRegex
        .r
        .findFirstIn(line)
        .getOrElse("Did Not match Regex")

      val code = line.split(",")(1).toLowerCase.trim

      //get the code from line, and map to message
      if (message.isEmpty)
          alertMap +=  (code -> "No Eng Message")
      else
          alertMap += (code -> message)

    }
    alertMap
  }

  //timestamp of the time the message comes in through kafka
  def mapper(value: String, timeStamp: String, errMapBC: Broadcast[Map[String, String]]): Record = {

    //how often this error occurred in this time window
    //value format ->  [ occurrence#  \\s  type  \\|  code ]

    //get the number of occurrences from the incoming value message
    val occur  = value.trim.split("\\s")(0).toInt

    //get the code from the incoming value message
    val err = value.split("\\|")(1).toLowerCase.trim

    //get the error map from the broadcast
    val errMap = errMapBC.value

    //get the message
    val message = errMap.getOrElse(err, "")

    //create a timestamp from the string
    val time = structureDate(timeStamp)

    Record(time,message, err, occur)
  }

  def main(args: Array[String]): Unit = {

    setupLogging
    //You can add devices here, or change your regex to grab diff data
    //regex doesnt deal with errors/warns. it just pulls otu good lines!
    val topics = Array("test")

    val hostPort = "localhost:9092"

    //alerts file
    val alertsCSV = "src/main/resources/broadcastfiles/Alerts.csv"

    //Spark Session for sql application and Dataset API methods
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SS_Kafka_MetricsDB")
      .getOrCreate

    //To access Dataset API
    import spark.implicits._

    //make it pick up data from a while ago, utilizing kafka offsets
    val dataFlow = spark
      .readStream
      .format("kafka")
      //here is the port def from the ./streamKafka script
      .option("kafka.bootstrap.servers", hostPort)
      .option("subscribe", topics.mkString)
      .option("startingoffsets", "latest")
      .option("failOnDataLost","false")
      .load

    //grab all the data from the topicValue
    val kafkaData = dataFlow
      .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS Timestamp)", "CAST(key as STRING)")
      .as[(String, String, String, String)]

    //broadcast the alertsFile
    val alertBroadcast = spark
        .sparkContext
        .broadcast(loadAlerts(alertsCSV))

    val errorsDS = kafkaData
      .map(x => mapper(x._2, x._3, alertBroadcast))

    //sink to the console uncomment above to see fileSink
    val consoleSink=errorsDS
      .select("*")
      //aggregate by time, $ explicitly says timestamp/message are columns
      .groupBy($"message", $"error")
      //We had to alias our columns here because the agg functions rename them to weird jargon
      .agg(sum("occurrence"))
      //count similar messages, not sure how this works but it does
      //must refer to as column with $ to use .desc method (most recent times otherwise
      //will look like its not updating
      .orderBy(desc("occurrence"))
      .writeStream
      .outputMode("complete")
      .format("console")

    //sink to IDE console
    consoleSink
      .start
      .awaitTermination
  }
}
