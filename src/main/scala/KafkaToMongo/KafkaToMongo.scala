package KafkaToMongo

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.mongodb.scala.{MongoClient, MongoDatabase}


object KafkaToMongo {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load
    val envProps: Config = conf.getConfig(args(0)) // Getting user defined parameters during runtime
    val sparkConf = new SparkConf().setMaster("local").setAppName("KafkaToMongo") // New Spark context
    val streamingContext = new StreamingContext(sparkConf, Seconds(envProps.getInt("window"))) // New Streaming context
    streamingContext.sparkContext.setLogLevel("WARN")

    System.setProperty("org.mongodb.async.type", "netty")
    val mongoClient: MongoClient = MongoClient(envProps.getString("mongo.uri")) // Creating a new mongo connection
    val database: MongoDatabase = mongoClient.getDatabase(envProps.getString("mongo.dbname"))

    val tfmsUrl = envProps.getString("tfs.url")

    val topicsSet = Set(envProps.getString("topic.name")) // Setting Kafka topic name

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> envProps.getString("bootstrap.server"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    ) // Parameters to Kafka consumer

    val tweetData: DStream[String] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    ).map(record => record.value)   // Getting messages from Kafka topic

    val tweetList = tweetData.map(line => {
      val namo_strings = List("narendra", "modi", "namo", "narendra modi") // Keywords to track (Extra level of filter)
      val raga_strings = List("rahul", "gandhi", "raga", "rahul gandhi") // Keywords to track (Extra level of filter)
      if (namo_strings.exists(line.toLowerCase.contains)) {
        //val tweet = "Narendra Modi"
        ("NarendraModi", 1) // Creating key:value pair from each sentence
      } else if (raga_strings.exists(line.toLowerCase.contains)) {
        //val tweet = "Rahul Gandhi"
        ("RahulGandhi", 1)
      } else {
        //val tweet = "Undefined"
        ("Undefined", 1)
      }
    }).reduceByKey(_ + _)

    val tweetSentiment = tweetData.map(line => {

      val namo_strings = List("narendra", "modi", "namo", "narendra modi")
      val raga_strings = List("rahul", "gandhi", "raga", "rahul gandhi")

      if (namo_strings.exists(line.toLowerCase.contains)) {

        val sentiment = GetSentiment.getSentiment(line, tfmsUrl) // Getting sentiment from Trained Tensorflow model

          if (sentiment==1){

            val sentimentType = "NMPositive"
            (sentimentType, 1) // Creating key:value pair from each prediction

          }else{

            val sentimentType = "NMNegative"
            (sentimentType, 1) // Creating key:value pair from each prediction

          }

      }else if (raga_strings.exists(line.toLowerCase.contains)) {

        val sentiment = GetSentiment.getSentiment(line, tfmsUrl)
        if (sentiment==1){

          val sentimentType = "RGPositive"
          (sentimentType, 1)

        }else{

          val sentimentType = "RGNegative"
          (sentimentType, 1)

        }
      }else {

        val sentimentType = "Undefined"
        (sentimentType, 1)

      }
    }).reduceByKey(_ + _)


    tweetList.foreachRDD { rdd =>

      val namo_count_1 = rdd.filter( x => x._1=="NarendraModi" ).map( y => y._2) // Extracting value from RDD using key
      val namo_count_2 = namo_count_1.collect() // Collecting count to driver

      val raga_count_1 = rdd.filter( x => x._1=="RahulGandhi" ).map( y => y._2) // Extracting value from RDD using key
      val raga_count_2 = raga_count_1.collect() // Collecting count to driver

      val namo_count_3 = namo_count_2.mkString("") // Converting to string
      val raga_count_3 = raga_count_2.mkString("")

      val namo_count_4 = if(namo_count_3 == "") 0 else namo_count_2(0) // If there is no tweet relating to a person then spark will return a null array.
      val raga_count_4 = if(raga_count_3 == "") 0 else raga_count_2(0) // But if Spark return null array we are returning 0

      println("Tweets related to Narendra Modi: " + namo_count_4)
      println("Tweets related to Rahul Gandhi: " + raga_count_4)

      SendCount.saveCount(namo_count_4, raga_count_4, database) // Writing results to db

    }


    tweetSentiment.foreachRDD { rdd =>

      val npsc_1 = rdd.filter(x => x._1 == "NMPositive").map(y => y._2).collect() // Extracting value from RDD using key
      val nnsc_1 = rdd.filter(x => x._1 == "NMNegative").map(y => y._2).collect()
      val rpsc_1 = rdd.filter(x => x._1 == "RGPositive").map(y => y._2).collect()
      val rnsc_1 = rdd.filter(x => x._1 == "RGNegative").map(y => y._2).collect()

      val npsc_2 = if (npsc_1.mkString("") == "") 0 else npsc_1(0)
      val nnsc_2 = if (nnsc_1.mkString("") == "") 0 else nnsc_1(0)
      val rpsc_2 = if (rpsc_1.mkString("") == "") 0 else rpsc_1(0)
      val rnsc_2 = if (rnsc_1.mkString("") == "") 0 else rnsc_1(0)

      println("Positive tweets related to Narendra Modi: " + npsc_2)
      println("Negative tweets related to Narendra Modi: " + nnsc_2)
      println("Positive tweets related to Rahul Gandhi: " + rpsc_2)
      println("Negative tweets related to Rahul Gandhi: " + rnsc_2)

      SendCount.saveSentimentCount(npsc_2, nnsc_2, rpsc_2, rnsc_2, database) // Writing results to db

    }

    //mongoClient.close()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
