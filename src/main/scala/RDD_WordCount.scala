import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import Utilities._
/** Count up how many of each word appears in a book as simply as possible. */
object RDD_WordCount {

  def wordFilter(word: String): Boolean = {
      word.equals("is") ||
      word.equals("you") ||
      word.equals("to") ||
      word.equals("your") ||
      word.equals("the") ||
      word.equals("a") ||
      word.equals("of")
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    //write a filter function that filters out stoplist words common to
    //english dictionary
/*    if (args.length == 0) {
      println("Need 1 Argument. Program Terminating")
      return -1;
    }*/
    // Set the log level to only print errors
    Logger
      .getLogger("org")
      .setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("WordCount")
      .master("local[8]")
      .getOrCreate()
    // Read each line of my book into an RDD, user picks file at cmd line
    val textFile = "words.txt" //access lists data (aka array)
    import spark.implicits._
    val input = spark
      .sparkContext
      .textFile(s"src/main/resources/RDDFiles/$textFile")
      .toDS()

    // Split into words separated by a regex that extracts words (word)
    // flatMap must return the same data type
    val words = input
      .flatMap(_.split("\\W+"))

    // Give all words the same case so they're case insensitive (word)
    val wordsLowerCase = words
      .map(_.toLowerCase)

    //deciding which words we don't care about
    val filteredWords = wordsLowerCase
      .rdd
      .filter(wordFilter)

    // Count up the occurrences of each word (word, 1) tuples
    val wordCounts = filteredWords
      .map((_,1))

    // Count the words, add them up by similar key (word, #Occurs) tuples
    val countKeys = wordCounts
      .reduceByKey((_+_))

    // Sort the words in descending order by the number of occurances
    val sortedWordOccurances = countKeys
      .sortBy(_._2, true)

    // Collect the results off the cluster
    val collectedResults = sortedWordOccurances
      .take(30);

    for (result <- collectedResults) {
      // ._ notation access tuples
      val word = result._1
      val occurance = result._2
      println(s"$word: $occurance")
    }
  }
}


