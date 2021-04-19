import java.io.{File, FileWriter}
import java.sql.Timestamp
import org.apache.spark.sql.functions._
import LogGenerator._
import org.apache.spark.sql._
import Utilities._
import scala.reflect.io.Directory

/**
  * Since we can pull a CSV off the Netsuite, we can create a structure of Records
  * And refer to their column header fields without regex! Fix this in MetricsDB
  * Note: We had to apply foldleft to fix the headers with parens () and change " " to "_"
  * cmd+option+L is reformat code on mac os derp
  *
  */

object RDD_RMAAgg {

  final case class Error_Record(
                                 Error: String,
                                 Count: Int
                               )

  final case class Customer_Count(
                                   Customer: String,
                                   Count: Int
                                 )

  final case class Record(
                           Internal_ID: String,
                           Related_Records: String,
                           Case: String,
                           Document_Number: String,
                           RMA_Status: String,
                           Sales_Effective_Date: String,
                           RMA_Customer_Name: String,
                           Return_Category: String,
                           Item: String,
                           Description: String,
                           Display_Name: String,
                           Sum_Of_Quantity: String,
                           Base_Price: String,
                           Applying_Transaction: String,
                           RMA_Memo: String,
                           RMA_Record_Created_Date: Timestamp,
                           RMA_Created_By: String
                         )

  def total_Failed_RMAs(spark: SparkSession, rangeStart: String, filter: String, failed: Boolean): DataFrame = {
    //execute sql queries for each of Chris's queries, 2 for starts.
    var x = ""
    if (failed) x = "Failed Unit" else x = ""

    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Case, Internal_ID, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")
      .where(
        s"""
           r.Return_Category like '%${x}%' AND
           r.Timestamp > '${rangeStart}'
          """)

      //show
      .agg(sum("Total").alias(filter))
  }

  //we learned IN clause cannot use wild card, *, :(, only like clause can use wild card
  def total_rev_failed_odu(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Internal_ID, Description, Document_Number
           from rma_data as r
           """)
      //Note that AND is parsed before OR
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like 'Failed Unit' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like 'Failed Unit' AND
           r.Display_Name like 'OS-1 Sensor' AND
           r.Timestamp > '${rangeStart}'
          """)
      //for testing why my script showed higher # of failed units (included "loaner / failed unit")
      //.groupBy("Display_Name", "Internal_ID", "Return_Category")
      .agg(sum("Total").alias(filter))
    //.orderBy("Internal_ID")
  }

  //considers failed noibox OS1-64
  def total_rev_failed_sinbon(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like 'Failed Unit' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like 'Failed Unit' AND
           r.Display_Name like '%Sinbon%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like 'Failed Unit' AND
           r.Display_Name like '%OS1-64 No iBox%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_rev_failed_gen2(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Description, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Failed Unit%' AND
           r.Description like '%GEN2%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_loaner_odu(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Loaner%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Eval%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Loaner%' AND
           r.Display_Name like '%OS-1 Sensor%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Eval%' AND
           r.Display_Name like '%OS-1 Sensor%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_loaner_sinbon(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Loaner%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Eval%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_loaner_gen2(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Description, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Loaner%' AND
           r.Description like '%GEN2%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Eval%' AND
           r.Description like '%GEN2%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_unused_odu(spark: SparkSession, rangeStart: String, filter: String): DataFrame = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Unopened%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Unopened%' AND
           r.Display_Name like '%OS-1 Sensor%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_unused_sinbon(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Unopened%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_unused_gen2(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Description, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")
      .where(
        s"""
           r.Return_Category like '%Unopened%' AND
           r.Description like '%GEN2%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_up_retro_pre_below_return_odu(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Preemptive%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Return for Upgrade%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Below Spec%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Retrofit%' AND
           r.Display_Name like '%ODU%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Preemptive%' AND
           r.Display_Name like '%OS-1 Sensor%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_up_retro_pre_below_return_sinbon(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Preemptive%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Return for Upgrade%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Below Spec%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Retrofit%' AND
           r.Display_Name like '%SINBON%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_up_retro_pre_below_return_gen2(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Internal_ID, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")
      .where(
        s"""
           r.Return_Category like '%Preemptive%' AND
           r.Description like '%%GEN2%%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Return for Upgrade%' AND
           r.Description like '%%GEN2%%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Below Spec%' AND
           r.Description like '%%GEN2%%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Retrofit%' AND
           r.Description like '%%GEN2%%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_recalibration(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Recalibration%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_wrong_sent(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%wrong sensor%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_unlabelled(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%None%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_swap_program(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%Swap%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_no_meet_expectation(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.Return_Category like '%meet customer req%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def total_RMA_Status_Cancelled(spark: SparkSession, rangeStart: String, filter: String) = {
    spark
      .sql(
        """
           Select Sum_Of_Quantity as Total, Return_Category, Display_Name, RMA_Status, Document_Number
           from rma_data as r
           """)
      .dropDuplicates("Document_Number")

      .where(
        s"""
           r.RMA_Status like '%Cancelled%' AND
           r.Timestamp > '${rangeStart}' OR
           r.Return_Category like '%Cancelled%' AND
           r.Timestamp > '${rangeStart}'
          """)
      .agg(sum("Total").alias(filter))
    //.orderBy("Timestamp")
  }

  def main(args: Array[String]): Unit = {

    //control the log spillage while program runs
    setupLogging

    //list of directories where data will be stored. This will create new, and delete old
    val list = List("CustomerCount", "errorData", "pendingRMADataAgg", "errorData", "pendingRMADataAgg", "ReasonCustomerCount", "pendingRMAInField", "gen2rmas", "gen1rmas", "pendingBySensorType", "specbreakdowngen2")
    for (name <- list) {
      val directory = new Directory(new File(s"src/main/resources/output/$name"))
      directory.deleteRecursively
    }

    val rmaFile = s"CasesonRMAResults_07_08.csv"
    //only add this if you want details on a specific month
    //note the output will show much fewer rma's, total will still contain all time rma's
    //format is yyyy-MM-dd HH:mm:ss
    val rangeStart = s""
    val beforeDate = s""

    //Spark Session for sql application and Dataset API methods
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("RDD_RMAAgg.scala")
      .getOrCreate

    //Forgot to import this shit to work with DS
    import spark.implicits._

    //create a datastructure from the csv file
    val df = spark
      .read
      .option("header", true)
      .csv(s"src/main/resources/RDDFiles/${rmaFile}")

    //replace the spaces with _ so we can create fields in the datastructure
    val fixedHeaderDf = df.columns
      .foldLeft(df)((curr, n) => curr
        .withColumnRenamed(n, n.replaceAll(" ", "_")))

    //create a proper Timestamp
    val ts = from_unixtime(unix_timestamp(col("RMA_Record_Created_Date"), "MM/dd/yyyy hh:mm a"), "yyyy-MM-dd HH:mm:ss")

    //create structured data with valid Timestamp
    val structuredDS = fixedHeaderDf
      .as[Record]
      .withColumn("TimeStamp", ts)

    //create a view to query with sql commands
    structuredDS.createOrReplaceTempView("rma_data")

    //total RMA's. Change false to true if we want to show only failed units. Subtract Total RMA's Cancelled, and use dropDuplicates to get true value
    total_Failed_RMAs(spark, rangeStart, "Total RMA's", false)
      .show

    //rev failed ODU/SINBON/GEN2
    val x = total_rev_failed_odu(spark, rangeStart, "Failed Odus").withColumn("id", monotonically_increasing_id())
    val y = total_rev_failed_sinbon(spark, rangeStart, "Failed SINBON").withColumn("id", monotonically_increasing_id())
    val z = total_rev_failed_gen2(spark, rangeStart, "Failed Gen2").withColumn("id", monotonically_increasing_id())
    val result1 = x.join(y, "id").join(z, "id").drop("id")
    result1
      .show

    //loaners ODU/SINBON/GEN2
    val x2 = total_loaner_odu(spark, rangeStart, "Loaner ODU").withColumn("id", monotonically_increasing_id())
    val y2 = total_loaner_sinbon(spark, rangeStart, "Loaner SINBON").withColumn("id", monotonically_increasing_id())
    val z2 = total_loaner_gen2(spark, rangeStart, "Loaner gen2").withColumn("id", monotonically_increasing_id())
    val result2 = x2.join(y2, "id").join(z2, "id").drop("id")
    result2
      .show

    //Unused ODU/SINBON/GEN2
    val x3 = total_unused_odu(spark, rangeStart, "Unused ODU").withColumn("id", monotonically_increasing_id())
    val y3 = total_unused_sinbon(spark, rangeStart, "Unused SINBON").withColumn("id", monotonically_increasing_id())
    val z3 = total_unused_gen2(spark, rangeStart, "Unused GEN2").withColumn("id", monotonically_increasing_id())
    val result3 = x3.join(y3, "id").join(z3, "id").drop("id")
    result3
      .show

    //Upgraded/Retrofit/Preemptive/belowspec/returnforupgrade ODU/SINBON/
    total_up_retro_pre_below_return_odu(spark, rangeStart, "Spec ODU").show
    total_up_retro_pre_below_return_sinbon(spark, rangeStart, "Spec SINBON").show
    total_up_retro_pre_below_return_gen2(spark, rangeStart, "Spec GEN2").show

    //recal / wrong sent / rmas cancelled
    total_recalibration(spark, rangeStart, "Recalibration").show
    total_wrong_sent(spark, rangeStart, "Wrong Sensor Sent").show

    //unlabelled / swap / no_meet / cancelled
    total_swap_program(spark, rangeStart, "Total Swap Program").show
    total_no_meet_expectation(spark, rangeStart, "Total Expectation Not Met").show
    total_RMA_Status_Cancelled(spark, rangeStart, "Total RMA's Cancelled").show

    //gets total # per Error of RMA type, first function total_Errors_For_RMA takes timestamp
    structuredDS
      .select("Case", "Sum_Of_Quantity")
      .where(s"""Timestamp > '${rangeStart}' AND Return_Category like '%Failed Unit%'""")
      .map(x => {
        //drops the case string, and the first space, to clean the data
        val y = x.toString.split("\\[|\\,|\\]|\\s|-").drop(2)
        //take everything except the last element, and convert it to a string which will be the key
        val p = y.dropRight(1).mkString(" ")
        //take last element (ie 2.0) convert to double, then to Int so we can agg on it
        val z = y.last.toDouble.toInt
        //return a record we can agg on (Error, Count)
        Error_Record(p, z)
      }
      )
      .toDF
      .groupBy("Error")
      .agg(sum("Count").alias("Occurrences"))
      .orderBy(desc("Occurrences"))
      .rdd
      .coalesce(1)
      .map(_.toString().replace("[", "").replace("]", ""))
      .saveAsTextFile("src/main/resources/output/errorData")

    //gets total # per Error of RMA type, first function total_Errors_For_RMA takes timestamp
    structuredDS
      .select("Case", "Sum_Of_Quantity")
      .where(s"""RMA_Status like '%Pending Receipt%' OR RMA_Status like '%Pending Approval%'""")
      .rdd
      .map(x => {
        val y = x.toString.split("\\[|\\,|\\]|\\s|-").drop(2)
        //take everything except the last element, and convert it to a string which will be the key
        val p = y.dropRight(1).mkString(" ")
        //take last element (ie 2.0) convert to double, then to Int so we can agg on it
        val z = y.last.toDouble.toInt
        //return a record we can agg on (Error, Count)
        Error_Record(p, z)
      }
      )
      .toDF
      .groupBy("Error")
      .agg(sum("Count").alias("Occurrences"))
      .orderBy(desc("Occurrences"))
      .rdd
      .coalesce(1)
      //remove brackets to write clean csv to file
      .map(_.toString().replace("[", "").replace("]", ""))
      .saveAsTextFile("src/main/resources/output/pendingRMADataAgg")

    //match customer to reason for RMA. Show who's rma'ing, and why (for Vince / France)
    //Reason, Customer, Count
    structuredDS
      .select("Case", "RMA_Customer_Name", "Sum_Of_Quantity", "Internal_ID", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp > '${rangeStart}'")
      .rdd
      .coalesce(1)
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/ReasonCustomerCount")

    //customer, count
    structuredDS
      .select("Case", "RMA_Customer_Name", "Sum_Of_Quantity", "Internal_ID", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp > '${rangeStart}'")
      .groupBy("RMA_Customer_Name")
      .agg(sum("Sum_Of_Quantity").alias("Sum"))
      .orderBy(asc("Sum"))
      .rdd
      .coalesce(1)
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/CustomerCount")

    //match customer to reason for RMA. Show who's rma'ing, and why (for Vince / France)
    structuredDS
      .select("Case", "RMA_Customer_Name", "Sum_Of_Quantity", "RMA_Record_Created_Date", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp < '${beforeDate}' AND RMA_Status like '%Pending Receipt%' OR Timestamp < '${beforeDate}' AND RMA_Status like '%Pending Approval%'")
      .orderBy(desc("RMA_Record_Created_Date"))
      .rdd
      .coalesce(1)
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/pendingRMAInField")

    //gen1 rma's
    structuredDS
      .select("Case", "RMA_Customer_Name", "Sum_Of_Quantity", "RMA_Record_Created_Date", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp > '${rangeStart}' AND Display_Name not like '%GEN2%'")
      .orderBy(desc("RMA_Record_Created_Date"))
      .rdd
      .coalesce(1)
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/gen1rmas")

    //gen2 rma's vs gen1
    structuredDS
      .select("Case", "RMA_Customer_Name", "Sum_Of_Quantity", "RMA_Record_Created_Date", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp > '${rangeStart}' AND Display_Name like '%GEN2%'")
      .orderBy(desc("RMA_Record_Created_Date"))
      .rdd
      .coalesce(1)
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/gen2rmas")

    //amount of rma's pending receipt (still in the field), organized by gen1 Odu, gen1 sinbon, os0,1,2 gen2
    structuredDS
      .select("Display_Name", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"RMA_Status like '%Pending Receipt%' AND Return_Category like 'Failed Unit' OR RMA_Status like '%Pending Receive%' AND Return_Category like 'Failed Unit'")
      .groupBy("Display_Name")
      .count
      .orderBy(desc("Count"))
      .rdd
      .coalesce(1)
      //get rid of CASE BS, and the negative on the count numbers
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", ""))
      .saveAsTextFile("src/main/resources/output/pendingBySensorType")

    //SPEC: Upgraded/Retrofit/Preemptive/belowspec/returnforupgrade GEN2
    //agg worked here because we didn't go by CASE name, since cases are all different. FUCK
    //What's the solution?
    structuredDS
      .select("Display_Name", "Return_Category", "Sum_Of_Quantity", "Document_Number")
      .dropDuplicates("Document_Number")
      .where(s"Timestamp > '${rangeStart}' AND Display_Name like '%GEN2%'")
      .groupBy("Display_Name", "Return_Category")
      .agg(sum("Sum_Of_Quantity").alias("Count"))
      .orderBy(desc("Count"))
      .rdd
      .coalesce(1)
      //only need to get rid of "-" for this one, no case number
      .map(_.toString.replaceAll("CASE\\d{6}|\\[|\\]|-", "").replaceAll(""," "))
      .saveAsTextFile("src/main/resources/output/specbreakdowngen2")
  }
}
