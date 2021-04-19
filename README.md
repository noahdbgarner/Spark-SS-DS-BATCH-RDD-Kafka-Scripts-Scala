#NEVER MEMORIZE SOMETHING YOU CAN LOOK UP
#HUMANS ARE PATTERN MATCHERS & ASSOCIATIVE & PROCEDURAL

##Key Points From Each Object:'
###SavingTweets.scala  & PrintTweets.scala (review of AverageTweetLength.scala & Hashtags)
1. Repartition(1): this was done because we were saving RDD to a textFile, and we wanted 1 file instead of however many partitions, since a single RDD can have multiple partitions on a cluster
2. ssc.checkpoint are for driver failures, ie puts computed data somewhere we can grab if driver failed or faulted
3. ssc.star and ssc.term are discussed in DStreams of README.md last class. They just keep the script running and listening to the endless stream until a termination by user or by code.stop()
4. basic format is 1. setuplogging & setupTwitterAPI 2. setup a StreamingContext (much like SparkContext or SparkSession for Datasets) 3. setup Receiver with TwitterUtils.createStream, 4. setup DStream with a map function that gets twitter stream texts
5. Played a lot with foreachRDD on Dstreams, reduce on pairRDD DStream, and windows. Coming to much better understanding
6. Shows difference between when to use foreachRDD and reduceByWindow. ie. use foreachRDD in ur DStream to send RDD info to a textFile and save, or to a DB
7. When repartition? IF writing to a DATABASE
###Tracking Top URL's Requested (LogParser.scala and newManReaTimeLogParser.scala & FilterLogsSimilarError.scala)
1. Learned alot about ncat, how to use it with -lk to receive incoming data from a text file
2. Learned it is very useful for testing, but we need to directly connect to a server with HTTP
3. So, we utilized adb connect ports 5555 on my device and Newman, and logcat cmd to send logging info to my device in a textfile. We then ran ncat against this textfile on a different port
ie this different port is the port we listen to in our spark script for continuously flowing data from the logs
4. We can use filter and map functions on a receiverDStream to make a DStream
5. nodemon app.js > ~/Documents/Logs/nodelogs.log 2> ~/Documen/Logs/errornodelogs.log
6. This is the command we can use to send log data that WOULD go to the console STDOUT to files. we can then run ncat on these files and listen to that port in our spark script
7. Note, we may have to use String interpolation with SparkSqL to piece out random #s generated, and then reduce on similar error/warning messages
8. ie       .sql(s"Select lines from data where lines like '%$linePiece1%$linePiece2%$linePiece3%'")
###Advanced DStream logic (LogAlarmer.scala)
1. Thinking Newman logs project should not be fixed, I accomplished what I wanted. We just need a working something. Moving on to bigger things now...
This is exactly right because we want to move onto structured streams with Sparksql!!!
2. Build prototypes. Build them fast. Team Lead was right... wow
3. Learned of countByValueAndWindow function which reduces on a single data RDD (This case type string), and adds a second element to the row, the count of which that string appears, and it does this by window time and 
4. LEarned of util.Try, comboed with .getOrElse(0). It represents a computation that may result in an exception or return a successdful computed value
ie he also uses util.Try to handle dividing by 0. This seems much more efficient than Option, None, Some
5. So why use Option[]? Because in functional languages its common to specificy in the method signature that there could be an exception or null value. util.Try does not do this.
6. Option[] is simply a special value that can occur in the absence of a value
###SparkSQL with Streams (LogSQL.scala)
1. Learned that mkString is specifically for converting an array to a String, and toString is for Objects like String and Int, not Data structures like array
2. Found a perfect use case for the Some & .getOrElse() pattern. Specifically, if u assume data will be somewhere, use Some, in case randomly it is not in that position
3. Assuming getOrCReate constantly gets after first creation during a stream application. Okay, also assuming 
###Structured Streaming (AccessLogSSSQL.scala, SSOnTCPSocket.scala (self project))
1. There is 1 RDD per batch interval with an arbitrary amount of rows, maybe even zero.
2. A DStream is a continuous sequence of RDD's, a batch contains 1 RDD
3. We force structure by using foreachRDD and converting the data to objects with fields, or importing structured files like JSON
4. We don't need a StreamingContext, or batch interval for Structured Streaming
7. On streaming data, sql queries <=> DataFrame API and scala operations <=> DataSet API
9. Can you not cache a structured stream? Guess not. Doesn't make sense to cache a continuous stream of data
10. Learned how the tail cmd could be useful to see the end of files. Now I get Venkat's intentions.
11. Learned that Kafka has fault tolerance for our streams, and TCP port streaming does not. TCP Port socket stream is only for testing, like what I am doing now
12. Learned about storagelevel, there are 3 parts to it, you can store in mem, disk, and you can serialize it
13. Apparently we cannot have a structuredReadStream and a sparkSession in the same program. No functionality. The way we would handle this is run two separate scripts on the log data listening to the same port
14. You need a consistent strong stream of data
###Side project: Java log generator + Send to TCP Port for SS Spark script
1. Wow we got the tcp java random log data to tcp socket working holy fuck that is so cool!!!
2. Test Script with dataset timestamp method of structured streaming
3. Google future of MLLib with neural networks
Note this would be even better than logFiles developed by Newman because you'd basically have your own logs... but how will we do this.. hmm
4. We misjudged this port tunneling from java output to spark input... how will we solve this?
5. Forget the streaming from the java program, test with ncat on logs in log directory, continue learning about streams, practice with SQL
6. Worked, we just needed to keep the socket open on the Java program. Derp. Command is [ncat -lk [IP] [PORT] | ncat -lk [IP] [PORT]]
###Side Project: NewmanSSTCP.scala, TestingJavaTCPDataStreams, Working with SS Semantics and ideas
1. $ converts new fields created through sql queries such as count into real DS columns
you would otherwise have had in object delcaration
2. Windows are much better in SS because there is no batch time. We can specify this ourselves
3. learned about the raw string in scala """ """. This is useful for grabbing any string with quotes data
3. Learned about Thread.sleep() for slowing program data down
4. Learned about LocalDateTime parsing, and more pattern matching with Java's regex library, JAva interoperability with Scala, very useful
5. Can parse a date time from any log with JAva.time library
6. SimpleDateFormat cannot parse nanoseconds, thats why we used the LocalDateTime lib. Will this work with the Scala SS? Idk..
###Kafka Streaming: KafkaStreamApp.scala + Side Project RDD_ErrorSeverity
. See 10/31/2019 for great knowledge on the kafka ecosystem and setup in terminal for the zookeeper and kafka servers
. We simply start a zookeeper server first, why? ITs a zoo, keeper. It manages kafka. Its the reason we can have a consumer in the terminal read data at the same time as our spark script reading data. We essentially had two 
. Been going very ham with kafka Streams. Almost have watermarking done on structured kafka streams, multiple devices.
. Before kafka, you had to create 3 readStreams to attach to 3 separate ports and read data from each port
? Whats the difference between keeping state, and tracking your data in a window?
A Do we really need watermarking since now we have kafka offsets?. YES, the offsets help us track and start over with reads
. I can set up nodejs server to read from cassandra DB, query it every 5 minutes to update with the real time data processed through streams
A Error messages not counting properly: They are, you forgot that windowing is posting new rows, but the old data will still be displayed!!!
. Learned about alias to rename dataframe columns. Cool!
. REMOVE THE MESSAGE LIMITER FOR PRODUCTION!! GET A CSQL GUI
. TO DO MORE MAPPING, CONVERT TO ANOTHER DATASET WITH ANOTHER CASE CLASS!!
. Streaming log data fixed. Simply rest kafka server
. We can only map on dataset streams, we cannot reduce on a dataset, requires another SQL aggregation
. Skipping stateful info and ML, will go back when I find a good way to integrate
. Finishing Spark class today, moving back to nodejs and bash scripts
. Found great use for broadcast variable, to quick lookup for errors we deem severe based on our commonErrors file that we create
. After a join you must rename your columns because there will otherwise be duplicate columns. We must be able to refer to them with a select statement
. Learned how to do multiple aggregations in a groupby, and alias the columns for cassandra data dump
. Fixed columns in db, added model, realm, IP, and two broadcast variables to map devicedata and commonErrors
###RDD_RMAAgg
.Learned why SparkSql is not as expressive as DataSet API, ie. You cannot have two where clauses in SparkSql because of the way it parses the String




###Cassandra DB
. See programming journal

###ISSUES
Distributing data to different tables from my script (fixed by utilizing several tables and calling several writes inside ForEachBatch function)

###TODOS
1. Integrate with cluster / microservices / kubernetes
2. Machine learning algorithms on realtime data
3. Continue Notes on tools that haven't been documented yet (TestingJavaTCPData, Kafka_MetricsDB, etc.)
