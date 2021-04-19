import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}

import Utilities._
/** This file will not compile because we didn't copy over the big data
  * YELP files from the challenge. This program takes a business ID from
  * the user, and recommends 10 other similar restuarants they might
  * want to review. Uses our own machine learning algorithm, kind of*/

//DATA CLEANSING FIRST!!!! KNOW WHAT YOU WANT< RID OF WHAT YOU DON'T!!
//recommendation system for companies to review given something a user
//already reviewed based on other people's reviews

//my hypothesis for why this is not working is because many people
//just review one restaurant, there aren't enough people reviewing
//the same 'pairs' of movies...
object RDD_ReviewSimilaritiesSQL {

  final case class Business(business_id: String, name: String)

  type iDNamePair = (String, String)

  def mapIDToNames(row: Row): iDNamePair = {
    (row(0).asInstanceOf[String].trim, row(1).asInstanceOf[String].trim)
  }

  type userIdBusinessIdRatingPair = (String, (String, Double))

  //create a tuple, RDD element contains (userID, (businessID, rating))
  def mapUserToIDAndStars(row: Row): userIdBusinessIdRatingPair = {
    (row(0).asInstanceOf[String].trim, (row(1).asInstanceOf[String].trim, row(2).asInstanceOf[Double]))
  }

  //note all filter predicates are Boolean in nature
  type BusinessRating = (String, Double)
  type UserRatingPair = (String, (BusinessRating, BusinessRating))

  def filterDuplicates(userRatings: UserRatingPair): Boolean = {
    val businessRating1 = userRatings._2._1
    val businessRating2 = userRatings._2._2

    val business1 = businessRating1._1
    val business2 = businessRating2._1

    //keep these only if the businessID's are different
    (business1 < business2)
  }

  //Parameter is of the form (String,((String,Double),(String,Double)))
  type ReviewAndRatingPairs = ((String, String), (Double, Double))

  def makeReviewIDPairs(userRatings: UserRatingPair): ReviewAndRatingPairs = {
    //extract business ratings
    val businessReviewPair1: BusinessRating = userRatings._2._1

    //this line might have fucked my whole thing up, this 2
    val businessReviewPair2: BusinessRating = userRatings._2._2

    //extract businessID and rating, separately
    val business1 = businessReviewPair1._1
    val business2 = businessReviewPair2._1
    val rating1 = businessReviewPair1._2
    val rating2 = businessReviewPair2._2
    ((business1, business2), (rating1, rating2))
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs): (Double, Int) = {
    var numPairs: Int = 0
    var sum_xx: Double = 0.0
    var sum_yy: Double = 0.0
    var sum_xy: Double = 0.0

    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2

      //add up all the rating multiplications
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }

    val numerator: Double = sum_xy
    val denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy)

    var score: Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    (score, numPairs)
  }

  //RDD goes in like: ((movieId, movieId), (score, numRatings))
  type SimmedBusinessRatings = ((String, String), (Double, Int))

  //fix this function if broken on jobs
  def filterWithSim(x: SimmedBusinessRatings, businessID: String): Boolean = {
    val scoreThreshold = 0.97
    val coOccurenceThreshold = 1.0
    val pair = x._1
    val sim = x._2
    (pair._1 == businessID || pair._2 == businessID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
  }

  def main(args: Array[String]): Unit = {

    //carl's jr.
    val businessID = "qJeSjOMgWB3er3UXG33ZVw"

    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    //dataset api notice the config is part of the session start
    val spark = SparkSession
      .builder
      .appName("WordCountJson")
      .master("local[8]")
      .getOrCreate

    //For Datasets
    import spark.implicits._

    //make our Dataframe with reviews.json
    val reviewsDF = spark
      .read
      .json("src/main/resources/RDDFiles/review.json")
      //action
      .cache

    //This needs to be RDD for minor operations (userID,(businessID,rating)
    val goodReviewsRDD = reviewsDF
      .select("user_id", "business_id", "stars")
      //action
      .rdd
      .map(mapUserToIDAndStars)

    //figure out exactly what this is doing to our RDD
    val joinedRatings = goodReviewsRDD
      .join(goodReviewsRDD)
    // RDD: userID => ((businessID, rating), (businessID, rating))
    // Filter out duplicate pairs
    // Filtering will remove lines we don't want
    val uniqueJoinedRatings = joinedRatings
      .filter(filterDuplicates)

    // Now key by (movie1, movie2) pairs. Rid userId don't care
    // Whenever we map, we are changing structure of each line
    val businessPairs = uniqueJoinedRatings
      .map(makeReviewIDPairs)
    // RDD: (business1, business2) => (rating1, rating2)

    // Now collect all ratings for each movie pair and compute similarity
    val businessPairRatings = businessPairs
      .groupByKey
    // RDD: ((business1, business2) = > (rating1, rating2), (rating1, rating2) ... )

    val businessPairSimilarities = businessPairRatings
      .mapValues(computeCosineSimilarity)
      //aciton
      .cache

    //test on cluster this action fucks everything
    println("\nPrinting some filtered results (This will take 15 minutes)")
    // Filter for movies with this sim that are "good" as defined by
    // our quality thresholds above

    //RDD: ((movie1, movie2), (score, numRatings)), Sort by quality score.
    val top10 = businessPairSimilarities
      .filter(x => filterWithSim(x, businessID))
      .sortBy(_._2._1, false)
      .take(10)

    //make our Dataset with business.json (kick ass omg)
    //objects so simply
    val businessDetailsDS = spark // spark session
      .read // get DataFrameReader
      .json("src/main/resources/RDDFiles/business.json")
      //select the keys we want for our Business object, names should match
      .select("business_id","name")
      //action
      .as[Business]
      .cache

    //this works, oh my god, extracted the string I need!!! AHHH!!!
    //This is the code I will need for the end, now I can do the rest
    businessDetailsDS
      .createOrReplaceTempView("names")
    //Get the business name from our view Dataframe
    val businessName = spark
      .sql(s"Select name from names where business_id like '$businessID'")
      //action
      .first

    //remember, once we have a row, we access items like a list and cast them
    println("Top 10 similar restaurants to review for " + businessName(0) + ":")
    for (result <- top10) {
      val pair = result._1
      val sim = result._2
      // Display the similarity result that isn't the movie we're looking at
      var similarMovieID = pair._2
      if (similarMovieID == businessID) {
        similarMovieID = pair._1
      }

      //get a similar business name from the
      val aRestaurant = spark
        .sql(s"Select name from names where business_id like '%$similarMovieID%'")
        .first
      //display results
      println(aRestaurant(0) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
    }

    readLine
    //think closing a connection as leaving your trash on a table after
    spark
      .stop
  }
}
