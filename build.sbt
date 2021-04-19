/**
  * NOTE: DEPENDENCY FILES ARE READ TOP DOWN
  * Learned: kafka depended on streaming API above it, so
  * kafka had to be listed as a dependency last!
  *
  * 
  * */

name := "SimpleStreamingScripts"

version := "1.2.3"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)
//must be above spark-sql-kafka must be above
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"

//This allows us to send data to cassandra, note that our cassandra server must be running
//note we also added a .jar file to this project, the spark 2.4 connector from github
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
//the datastax is not recognized by intellij, but the script does.
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.2"
