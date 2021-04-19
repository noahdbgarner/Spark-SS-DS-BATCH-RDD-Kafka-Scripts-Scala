import java.sql.Timestamp
import java.time.LocalDateTime
import java.util.regex.Pattern

import LogGenerator._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
/**
  *
  * A DEMO OF KAFKA !BATCH! STREAMING, NOTICE THE STREAMING CONTEXT
  * IE NOT REAL TIME STREAMING
  * THIS SCRIPT IS YOUR CONSUMER
  */

object Batch_KafkaIntegration {

  case class Record(topic: String, timestamp: Timestamp, message: String, charCount: Int)

  //Row is a Record from an RDD from a RRT stream
  def mapDeskLogs(topic: String, data: String, dateRegex: String, messageRegex: String): Record = {

    //okay lets work with regex to find Date **-** and the data we need
    val dateMatcher = Pattern.compile(dateRegex).matcher(data)
    val messageMatcher = Pattern.compile((messageRegex)).matcher(data)

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
      Record(topic, Timestamp.valueOf(localDateTime), messageString, messageString.length)
    }

    //handle the case that our matcher didn't work out
    else Record(topic, null, "bad message", 0)

  }

  def main(args: Array[String]): Unit = {

    //set log level ERROR. This program has a ton of parameters damn
    setupLogging

    println("Program Started.")

    //what is the format of the date you're parsing? IS it between <>? This makes it easy
    //Ask for people to put data between these types of signals we'll call them
    val deskDateFormat = "(\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{1,6})"
    val ndmDateFormat = "\\<(.*)>"
    val messageRegex = "((?<=[I|D|E])(.*))"

    val regexList =
      List(
        ndmDateFormat,
        deskDateFormat,
        messageRegex
      )

    val ssc = new StreamingContext(
      "local[8]",
      "Batch_KafkaIntegration",
      //batch interval, adhustable
      Seconds(1)
    )

    //for batch sql queries
    val spark = SparkSession
      .builder
      .master("local[8]")
      .getOrCreate

    //so we can use .toDS or .toDF, most importantly with .as[Business] type things
    import spark.implicits._

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "100",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    //Newman Devices
    val topics = Array("Kitchen", "Basement", "Attic")

    //data stream with topics as the keys
    val stream = KafkaUtils
      .createDirectStream(
        ssc,
        //distribute partitions of data evenly
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

    val kafkaDataStamped = stream
      .map(record =>
        mapDeskLogs(record.topic, record.value, regexList(1), regexList(2))
      )
      //filterNot method, filters bad messages
      .filter(!_.message.equals("bad message"))

    //sorts by message size in each RDD of the DStream
    val kafkaRecords = kafkaDataStamped
      .transform(_.sortBy((_.charCount), false))

    //pull message out of our object and reduce on it, keeping record
    val mappedAverages = kafkaRecords
      .map(x => (x.message, x))
    //inverse reduce function is not working

/*    //just a test
    val avgInWindow = mappedAverages
      .map(x => (x._2.topic, x._2.charCount))

    //this takes the average, and the inverse average
    val reduced = avgInWindow
      .reduceByKeyAndWindow(
        (x, y) => {
          (x + y) / 2
        },
        (x, y) => {
          (2 * x) - y
        },
        Seconds(5),
        Seconds(1)
      )

    reduced.foreachRDD((rdd, time) => {
      rdd.collect.foreach(println)
    })
*/

    mappedAverages.foreachRDD((rdd, time) => {

      //makes shuffling go away, good optimization
      val repartitionedRDD = rdd.repartition(1)
        .cache
      //only save this rdd has data in it otherwise forget it
      if (repartitionedRDD.count > 0) {
        //lets only work with the object, this makes it so we can use sql~
        val datasetOfRecords = rdd
          .map(_._2)
          .toDS

        //temp table to query
        datasetOfRecords
          .createOrReplaceTempView("LogData")

        //where filter
        val condition = "error"

        //gives avg chars where error message occurs, in each device attached to Kafka, ordered  desc
        val queriedDataframe = spark
          .sql(s"Select topic, avg(charCount) as AverageChars from LogData where LOWER(message) like '%$condition%' group by topic order by AverageChars")

        //show results
        println(s"========= $time =========")
        queriedDataframe
          .show

      }
    })

    //required with a sliding window (reduceByWindow ops)
    ssc
      .checkpoint("src/main/checkpoints/data")

    ssc
      .start

    ssc
      .awaitTermination
  }
}
