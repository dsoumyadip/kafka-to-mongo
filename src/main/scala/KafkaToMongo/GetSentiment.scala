package KafkaToMongo

import com.typesafe.config.{Config, ConfigFactory}
import play.api.libs.json._
import scalaj.http._

object GetSentiment {

  def getSentiment(sentence: String, tfmsUrl: String): Int = {

    val sentenceToPredict = "{\"inputs\":{\"sentence\": \"" + sentence.toLowerCase.replaceAll("\\W", "") + "\"}}" //Replacing all special character
    val response = Http(tfmsUrl).postData(sentenceToPredict).asString.body
    val JsValue_json_string: JsValue = Json.parse(response)
    val sentiment: Int = (JsValue_json_string \ "outputs" \ "classes" \ 0).as[Int] // Extracting sentiment from nested json response
    return  sentiment
  }
}
