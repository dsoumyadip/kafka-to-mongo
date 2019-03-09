# KafkaToMongoDB(Spark Streaming)
This application demonstrate how to fetch data from Kafka topic, get sentiments of tweets and aggregate count based on sentiment.

## Window time:
10 seconds

## Dependencies:

Scala 2.11.x

Spark 2.3.2

## Building Jar:
Package as a FAT jar file.

 $ sbt clean
 
 $ sbt compile
 
 $ sbt assembly

## To run the application:
$ spark-submit --master local[2] --class KafkaToMongo.KafkaToMongo --conf spark.ui.port=12901 target/scala-2.11/KafkaToMongo-assembly-1.0.jar prod

