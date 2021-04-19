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
  * Author: Noah Garner
  * Init Date: Nov 20, 2019
  * This project is way over engineered. It does more than it needs to do but
  * that's the point for my learning. integrate everything. take out what you
  * don't need
  * Kafka makes it very easy to differentiate between multiple data flows in a
  * script like the one we just wrote
  */

object SS_KafkaCassandraInt {

  //Structure for our data, timestamp will track nanoseconds.
  //time string tracks milliseconds, code noted in our CommonErrors textfile
  case class Record(topic: String,
                    timestamp: Timestamp,
                    message: String,
                    charcount: Option[Int],
                    time: String,
                    code: Int,
                    ip: String,
                    realm: String,
                    model: String)

  //after querying it, we will have a window time period, topic, data (truncated errors), and occurrences of errors
  case class RecordMessagesTruncated(window: String,
                                     topic: String,
                                     message: String,
                                     occurrences: BigInt,
                                     ip: String,
                                     realm: String,
                                     model: String,
                                     code: BigInt)

  //Too complicated to be a method parameter
  type BCDeviceData = Broadcast[Map[String, List[String]]]
  type BCErrorData = Broadcast[Map[String, Int]]

  //String is the value from record in a topic from a RRT stream (kafka)
  def mapToRecord(topic: String,
                  data: String,
                  time: String,
                  errorMapBC: BCErrorData,
                  deviceDataMapBC: BCDeviceData): Record = {

    val dateRegex = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{0,6})"
    val messageRegex = "((?<=[IDEWD]\\D{0,100}?:)(.*))"

    val date = "2019-" + dateRegex
      .r
      .findFirstIn(data)
      .getOrElse(null)
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

    //grab the Map from the broadcast wrapper, and get the code of messageString, otherwise its 0
    val errMap = errorMapBC.value
    val code = errMap.getOrElse(messageString, 0)

    //grab the Map from the broadcast wrapper, device data info
    val dataMap = deviceDataMapBC.value
    val deviceData: List[String] = dataMap.getOrElse(topic, List[String]())
    //this list contains at indicies 0:IP, 1:Realm, 2:Model (trim for cassandra)
    val ip = deviceData(0).trim
    val realm = deviceData(1).trim
    val model = deviceData(2).trim
    //since it could be null, handles this
    val messageLength = Some(messageString.length)

    //creates a Record Object with Timestamp field, generalized message no #s, and message's length
    Record(topic, dateTime, messageString, messageLength, time, code, ip, realm, model)
  }

  //(helper) loads known errors into a map that will be broadcasted for use on nodes
  def loadCommonErrors(file: String): Map[String, Int] = {
    //Handle character encoding issues for when on a node in the cluster
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create empty map to fill
    var errorMap: Map[String, Int] = Map()
    val errorFile = Source.fromFile(s"$file").getLines
    //load the map with data in errorFile
    for (line <- errorFile) {
      val parsedLine = line.split(",")
      val error = parsedLine(0)
      val code = parsedLine(1).trim.toInt
      errorMap += (error -> code)
    }
    //returns the map
    errorMap
  }

  //(helper) loads known errors into a map that will be broadcasted for use on nodes
  def loadDeviceData(file: String): Map[String, List[String]] = {
    // Handle character encoding issues for when on cluster: (woops)
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create empty map to fill
    var deviceDataMap: Map[String, List[String]] = Map()
    val deviceDataFile = Source.fromFile(s"$file").getLines
    //load the map with data in errorFile
    for (line <- deviceDataFile) {
      var data = new ListBuffer[String]()
      val parsedLine = line.split(",")
      //the device name is at index 0, see IPandDeviceNames.txt file
      val deviceName = parsedLine(0)
      //add the rest of the data to a list, type String
      data += parsedLine(1)
      data += parsedLine(2)
      data += parsedLine(3)
      //add a Device -> Data to the map
      deviceDataMap += (deviceName -> data.toList)
    }
    //returns the map
    deviceDataMap
  }

  def main(args: Array[String]): Unit = {
    println("Program Started.")
    //set log level ERROR, reduce console spam for development
    setupLogging

    //Newman Devices to differentiate as data is produced.
    //You can add devices here, or change your regex to grab diff data
    //regex doesnt deal with errors/warns. it just pulls otu good lines!
    val topics = Array("hardreboot_ft","castreboot_ft","kitchen", "basement", "attic", "random")

    //These files will contain the data you know in advance, including device info
    //errors file
    val commonErrorsFile =
      """src/main/resources/broadcastfiles/ImportantErrors.txt"""
    //devicedata  file
    val deviceDataFile =
      """src/main/resources/broadcastfiles/IPandDeviceNames.txt"""

    //assign port to transfer data with
    val HOSTNAME = "127.0.0.1"
    //DB name on cassandra test cluster
    val keyspace = "newman"
    //cassandra cluster name from terminal
    val clustername = "Noahs Cluster"
    //table name
    val tablename = "devicedata"

    //Spark Session for sql application and Dataset API methods
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("SS_KafkaCassandraInt")
      .getOrCreate
      .setCassandraConf(clustername, CassandraConnectorConf.ConnectionHostParam.option(HOSTNAME))

    //not sure this is correct but we will test later, loadMovieNames returns a map
    val errorRecords = spark
      .sparkContext
      .broadcast(loadCommonErrors(commonErrorsFile))

    //broacast the IP and Device Name to map into objects' topics
    val deviceData = spark
      .sparkContext
      .broadcast(loadDeviceData(deviceDataFile))

    //To access Dataset API
    import spark.implicits._

    //make it pick up data from a while ago, utilizing kafka offsets
    val dataFlow = spark
      .readStream
      .format("kafka")
      //here is the port def from the ./streamKafka script
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", topics.mkString(", "))
      .option("startingoffsets", "latest")
      .option("failOnDataLost","false")
      //do not set group.id with structured streaming it does this automatically
      .load

    //grab all the data from the topicValue
    val kafkaData = dataFlow
      .selectExpr("CAST(topic AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS STRING)", "CAST(key as STRING)")
      .as[(String, String, String, String)]

    val kafkaDataStamped = kafkaData
      //we will find out how to get topic from SS later lets get this working
      .map(x => mapToRecord(x._1, x._2, x._3, errorRecords, deviceData))
      //filter records that didn't match our date regex or message regex
      //and are must be less than 60 chars for Cassandra debugging
      .filter(x => !x.message.equals("bad message") && x.message.length < 60)

    //Always takes errors/warning. Edit this to match with ErrorsNotIn file
    val countedMessagesWithWindow = kafkaDataStamped
      .select("*")
      .withWatermark("timestamp", "20 seconds")
      //aggregate by time, $ explicitly says timestamp/message are columns
      .groupBy(window($"timestamp", "2 seconds"), $"topic", $"message", $"ip", $"realm", $"model")
      //We had to alias our columns here because the agg functions rename them to weird jargon
      .agg(count("message").alias("occurrences"), sum("code").alias("code"))
      //count similar messages, not sure how this works but it does
      //must refer to as column with $ to use .desc method (most recent times otherwise
      //will look like its not updating
      .orderBy($"window".desc, $"occurrences".desc)
      //we only want data that contains error or warning, otherwise we can get all data
      //multiline strings with """ like python example
      .where(
      s"""
           LOWER(message) like '%error%' OR LOWER(message) like '%warning%'
        """)

    //dataset to do more aggregations. Can not do any further aggregations
    //however, couldn't we do mapreduce or sparksql?
    val structMessagesWithWindow = countedMessagesWithWindow
      .as[RecordMessagesTruncated]

    //This gets the first time in the window, for DB purposes, fix this
    //into function, then move to bash program
    //x is an RDD, then map is called on this RDD, y is a line in the RDD
    //TODO: Investigate how, and when we would use a UDF instead of transform? Is it because we aren't creating a new column?
    //Note regex replace would not solve
    //Actually, I believe a UDF is perfectly suitable instead. Why? We wouldn't need the case class!
    val countedMessagesAtTimeIntervals = structMessagesWithWindow
      .transform(
        x =>
          x.map(y =>
            RecordMessagesTruncated(
              //we did this because window originally gives two values.. begin and end of window
              //this split takes [xxxtime which is (0), and removes [ to give window the beginning time only. Genius. Wow I was smart
              y.window
                .split(",")(0)
                .replace("[", ""),
              y.topic,
              y.message,
              y.occurrences.toInt,
              y.ip,
              y.realm,
              y.model,
              y.code.toInt)
          )
      )

    //put data into Cassandra. Needs fixing, only complete is working
    val query = countedMessagesAtTimeIntervals
      .writeStream
      .outputMode("complete")
      //working, thank god we're doing a 1 min trigger, spikes make sense
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .foreachBatch((batchDF, id) => {
        //you can convert batchDF to .rdd and apply
        //rdd methods then saveToCassandra() here..
        //batchDF.rdd.saveToCassandra("KS","TBLE",SomeColumns("name","name")

        //write newman device data to a devicedatatable
        batchDF
          .select("*")
          .where(
          """
              topic like '%attic%' OR
              topic like '%kitchen%' OR
              topic like '%basement%' OR
              topic like '%castreboot_ft%' OR
              topic like '%hardreboot_ft%'
            """)
          .write
          //we can write to different tables per the topic, ie device name
          .cassandraFormat(tablename, keyspace)
          .option("cluster", clustername)
          //only mode is append
          .mode("append")
          .save
        //uncomment when testing with multi table write to database
        /*     batchDF
                  .select("*")
                  .where("""
                      topic like '%Random%'
                   """)
                  .write
                  //we can write to different tables per the topic, ie device name
                  .cassandraFormat("javadata", keyspace)
                  .option("cluster", clustername)
                  //only mode is append
                  .mode("append")
                  .save
                */
      }
      )

    //uncomment below to dump massive amounts of accumulated data to file
    //.option("checkpointLocation", "src/main/checkpoints/data")
    //for simple testing we can append format("console") here!! sweet!!
    //.format("console")

    query
      .start
      .awaitTermination
  }
}
