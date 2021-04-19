import Utilities._
import LogGenerator._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
/** Monitors a stream of Apache access logs on port 9999, and prints an alarm
 *  if an excessive ratio of errors is encountered.
  *  So the processor on this computer was too good, so we had to make the log file up to
  *  2.5 million lines for the windowing effect to be noticed
  *
  * /** Formmated for the Streaming course AccessLog File in /Logs Directory */
 */
object Batch_AccessLogAlarmer {
  
  def main(args: Array[String]) {

    val PORT = 9999
    val HOSTNAME = "127.0.0.1"

    setupLogging

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogAlarmer", Seconds(1))

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc
      .socketTextStream(HOSTNAME, PORT, StorageLevel.MEMORY_AND_DISK_SER)
    
    // Extract the status field from each log line
    val statuses = lines
      .map(_.split(" ")(8))
      .filter(_.length < 210)

    statuses.print(5)
    
    // Now map these status results to success and failure
    val successFailure = statuses.map(x => {
      val convert = Some(x.toInt)
      //getOrElse always grabs the values in Option or Try wrappers...
      //ohhh so getOrElse gives us a default value, ie we don't worry about none!
      val statusCode = convert.getOrElse(0)

      if (statusCode >= 200 && statusCode < 300) {
        "Success"
      } else if (statusCode >= 500 && statusCode < 600) {
        "Failure"
        //handles x.toInt not being convertible, therefore its 0 and goes here
      } else {
        "Other"
      }
    })
    
    // Tally up statuses over a 5-minute window sliding every second
    val statusCounts = successFailure
      .countByValueAndWindow(Seconds(300), Seconds(1))
    
    // For each batch, get the RDD's representing data from our current window
    statusCounts.foreachRDD((rdd, time) => {
      // Keep track of total success and error codes from each RDD
      var totalSuccess:Long = 0
      var totalError:Long = 0

      if (rdd.count() > 0) {
        val elements = rdd.collect
        for (element <- elements) {
          val result = element._1
          val count = element._2
          if (result == "Success") {
            totalSuccess += count
          }
          if (result == "Failure") {
            totalError += count
          }
        }
      }
      // Print totals from current window
      println("Total success: " + totalSuccess + " Total failure: " + totalError)

      // Don't alarm unless we have some minimum amount of data to work with
      if (totalError + totalSuccess > 100) {
        // Compute the error rate
        // Note use of util.Try to handle potential divide by zero exception
        val ratio:Double = util
          .Try( totalError.toDouble / totalSuccess.toDouble ).getOrElse(0)
        // If there are more errors than successes, wake someone up
        if (ratio > 0.5) {
          // In real life, you'd use JavaMail or Scala's courier library to send an
          // email that causes somebody's phone to make annoying noises, and you'd
          // make sure these alarms are only sent at most every half hour or something.
          println("Wake somebody up! Something is horribly wrong.")
        } else {
          println("All systems go.")
        }
      }
    })
    
    // Also in real life, you'd need to monitor the case of your site freezing entirely
    // and traffic stopping. In other words, don't use this script to monitor a real
    // production website! There's more you need.
    
    // Kick it off
    ssc
      .checkpoint("src/main/resources/checkpoints/files")
    ssc
      .start
    ssc
      .awaitTermination

  }
}
