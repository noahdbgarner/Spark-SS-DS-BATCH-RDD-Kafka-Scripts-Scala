import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

import scala.io.{Codec, Source}
import Utilities._
/** Example of how we can use a broadcast variable, generally a Map that has
  * key -> Value pairs from a textFile small enough to be put on each individual
  * node in a cluster. This makes it much easier for lookup of data on the
  * nodes before we take an action upon the RDD
  * Popular superheroes mapped to most friends
  *
  *
  */

object RDD_PopularSuperHeroes {

  //eventually our columns. i.e. col1 = heroID col2 = numFriends
  //but we create an object so that we can call DS to structure our file data
  final case class Hero(heroID: Int, numFriends: Int)

  //map the RDD to tuple where column 1=(the hero ID), column 2=(#friends)
  //use the data to instantiate an immutable object from case class Hero()
  def mapper(line:String): Hero = {
    val parsedLine = line
      .split("\\s+")

    Hero(parsedLine(0).trim.toInt, parsedLine.length-1)
  }

  //create a map of heroes from our Marvel-names.outFile doc... how do we do this?
  def loadHeroesIntoMap: Map[Int, String] = {
    // Handle character encoding issues for when on cluster: (woops)
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //because I love this String interpolation
    val fileName = "Marvel-names.outFile"

    //now we need to get our file with the data loaded correctly
    val heroFile = Source.fromFile(s"src/main/resources/RDDFiles/$fileName").getLines

    //first we want to create an empty map
    var heroMap:Map[Int, String] = Map()

    //lastly, we need to go through each line of heroFile, and put it into our map
    //and we can split on ", but we need to shave the space off the first val
    for (line <- heroFile) {
      //create Array for structure
      val parsedLine = line.split('\"')
      //check it is valid for insertion to map (absolutely necessary or program crashes
      if (parsedLine.length > 1) {
        val heroId = parsedLine(0).trim.toInt
        val heroName = parsedLine(1)
        heroMap += (heroId -> heroName)
      }
    }
    heroMap
  }


  def main(args: Array[String]): Unit = {
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("PopularSuperHeroesDataSets")
      .master("local[8]")
      .getOrCreate

    val textFile = "Marvel-graph.outFile"


    //now we have a SparkSession, lets grab our unstructured data
    val unstructuredData = spark
      .sparkContext
      .textFile(s"src/main/resources/RDDFiles/$textFile")

    //structure our id->name file into a Map data structure, and broadcast
    //to our cluster to avoid partitioning it more than needed
    val heroMap = spark
      .sparkContext
      .broadcast(loadHeroesIntoMap)


    //So we can create our Dataset for use with Dataframes, must change. Create case class
    import spark.implicits._
    val structuredDataDS = unstructuredData
      .map(mapper)
      .toDS
      .cache //woops forgot this, most importantly

    //We have now have our Dataset, it is now structured, cache since huge computation
    //I can't perform reduceBy on an object, we have to implement this with Datasets
    //lets create a view instead and sql query it, be much easier (not to a val)
    //forgot, spark sql saves views to the spark object and we query from there, WORKING!!!!
    structuredDataDS
        .createOrReplaceTempView("PopularHeroes")
    val popularHeros = spark
        .sql("select heroID, SUM(numFriends) as sumFriends from PopularHeroes group by heroID order by SUM(numFriends) DESC")
        .cache
    popularHeros.printSchema()
    popularHeros
      .show(10)
    //connect with our super hero map
    val top10 = popularHeros
      //action, no longer a DF
      .take(10)

    var x: Int = 1
    //now we need to match them back into our dict, we will see if we can get this working with Dataset later
    for (result <- top10) {
      //access row's just like list elements, except you must cast them
      val heroID = result(0).asInstanceOf[Int]
      val heroFriends = result(1).asInstanceOf[Long]
      val heroName = heroMap.value(heroID)
      println(x+". "+heroName + " Friends: "+heroFriends )
      x+=1
    }

    //now, can I do this with dataframes? Or is this strictly when its good to use RDD's instead of DF?
    val popularHeroesDS = structuredDataDS
        .groupBy("numFriends")
        .sum()
        .orderBy(desc("numFriends"))

    popularHeroesDS
      .show(10)

    //method for keeping the web UI open
    readLine
    spark
      .stop
  }
}
