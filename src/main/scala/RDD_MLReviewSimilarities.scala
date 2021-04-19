import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{Row, SparkSession}
import Utilities._
/** A less accurate script utilizing ML to predict top 10 similar restaraunts
  * that a user may wany to review
  *
  */

object RDD_MLReviewSimilarities {

  //This business_id needs to be an integer, thats the issue!
  final case class Business(business_id: Int, name: String)

  def mapRowToTuple(row: Row): (String, String, Double) = {
    (row(0).asInstanceOf[String], row(1).asInstanceOf[String], row(2).asInstanceOf[Double])
  }

  def mapRowToTupleBusiness(row: Row): (String, String) = {
    (row(0).asInstanceOf[String], row(1).asInstanceOf[String])
  }

  //convert strings to there integer hashcodes
  //The ANDing with 0x7FFFFF is a bitmask,  makes our value always positive.
  //obviously needed for the integer thing... oh my god, I should've just
  //used this in my other program fuck!!!
  //There could accidentally be hash collision, two Strings resulting
  //in the same hash. Need distinct
  def hashStringToInt(tag: String) = tag.hashCode & 0x7FFFFF

  def main(args: Array[String]): Unit = {

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReviewSimilarities")
      .master("local[8]")
      .getOrCreate

    //for DF logic
    import spark.implicits._

    val reviewDF = spark
      .read
      .json("src/main/resources/RDDFiles/review.json")

    //use what we already used before to calculate the recommendations
    val reviewRDD = reviewDF
      .select("user_id", "business_id", "stars")
      .rdd
      .map(mapRowToTuple)
      .cache

    //create Integer ID's from String, and make a rating object
    //Rating(Int, Int, Double)
    val reviewRDDIntIDs = reviewRDD
      .map(x => Rating(hashStringToInt(x._1), hashStringToInt(x._2), x._3))
      .cache

    // Build the recommendation model using Alternating Least Squares
    println("\nTraining recommendation model...")

    val rank = 8
    val numIterations = 20

    val model = ALS
      .train(reviewRDDIntIDs, rank, numIterations, 0.01)

    val user = "hG7b0MtEbXx5QzbzE6C_VA"

    val userID: Int = hashStringToInt(user)

    //set up our id->name mapper
    val businessDF = spark
      .read
      .json("src/main/resources/RDDFiles/business.json")
      .cache

    val businessRDD = businessDF
      .select("business_id", "name")
      .rdd
      .map(mapRowToTupleBusiness)

    val businessIntIDs = businessRDD
      .map(x => (hashStringToInt(x._1), x._2))
      .toDF("business_id", "name")
      .as[Business]
      .cache

    businessIntIDs
      .createOrReplaceTempView("names")

    println("\nRatings for user ID " + userID + ":")

    val userRatings = reviewRDDIntIDs
      .filter(_.user == userID)

    val myRatings = userRatings
      .collect

    for (rating <- myRatings) {
      val similarBusinessID = rating.product.toInt
      //get a similar business name from the
      val businessName = spark
        .sql(s"Select name from names where business_id like '%$similarBusinessID%'")
        .first

      println(businessName(0) + ": " + rating.rating.toString)
    }

    println("\nTop 10 recommendations:")

    val recommendations = model.recommendProducts(userID, 10)
    for (recommendation <- recommendations) {
      val similarBusinessID = recommendation.product.toInt
      val businessName = spark
        .sql(s"Select name from names where business_id like '%$similarBusinessID%'")
        .first
      println(businessName(0) + " score " + recommendation.rating)
    }

    readLine
    spark
      .stop

  }
}
