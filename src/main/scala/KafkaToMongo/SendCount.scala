package KafkaToMongo

import org.mongodb.scala._
import java.util.Date

object SendCount {

  def saveCount(namo_count: Int, raga_count: Int, database: MongoDatabase): Unit = {

    val collection: MongoCollection[Document] = database.getCollection("tweet_count") // Fetching collection
    val currentTimestamp = new Date()
    val doc: Document = Document("timestamp" -> currentTimestamp, "namo_count" -> namo_count, "raga_count" -> raga_count)
    val observable: Observable[Completed] = collection.insertOne(doc) // Inserting count in db

    observable.subscribe(new Observer[Completed] {

      override def onNext(result: Completed): Unit = println("Inserted")

      override def onError(e: Throwable): Unit = println("Failed")

      override def onComplete(): Unit = println("Completed")

    })
  }

  def saveSentimentCount(namo_positive: Int, namo_negative: Int, raga_positive: Int, raga_negative: Int,database: MongoDatabase): Unit = {

    val collection: MongoCollection[Document] = database.getCollection("sentiment_count") // Fetching collection
    val currentTimestamp = new Date()
    val doc: Document = Document("timestamp" -> currentTimestamp, "namo_positive" -> namo_positive, "namo_negative" -> namo_negative, "raga_positive" -> raga_positive, "raga_negative" -> raga_negative)
    val observable: Observable[Completed] = collection.insertOne(doc) // Inserting count in db

    observable.subscribe(new Observer[Completed] {

      override def onNext(result: Completed): Unit = println("Inserted")

      override def onError(e: Throwable): Unit = println("Failed")

      override def onComplete(): Unit = println("Completed")

    })
  }

}
