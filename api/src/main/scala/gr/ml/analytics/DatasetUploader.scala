package gr.ml.analytics

import akka.actor.ActorSystem
import akka.actor.FSM.Failure
import akka.actor.Status.Success
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{Marshal, Marshaller}
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.domain.Rating
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.JsonSerDeImplicits._
import gr.ml.analytics.util.Util

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global


/**
  * Command line tool used to download movielens dataset
  * to local file system and upload ratings from it to the recommender service
  * via REST client.
  *
  * See https://grouplens.org/datasets/movielens
  */
object DatasetUploader extends App with Constants {

  Util.loadAndUnzip()

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val config: Config = ConfigFactory.load("application.conf")
  val uri = s"${config.getString("service.recommender.rest")}/ratings"

  val reader = CSVReader.open(ratingsPath)

  def postRating(rating: Rating): Future[HttpResponse] = {
    val future = Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = uri,
      // TODO fix this UGLY thing related to marshalling. Send a batch of entities per request.
      entity = HttpEntity(ContentTypes.`application/json`, "[" + ratingFormat.write(rating).toString() + "]")))

    future.onFailure{case e => e.printStackTrace()}
    future
  }

  val ratings = reader.toStreamWithHeaders.flatMap(map => {
    for {
      userId <- map.get("userId")
      itemId <- map.get("movieId")
      rating <- map.get("rating")
    } yield Rating(userId.toInt, itemId.toInt, rating.toDouble)
  }).toList

  val responseFutures = ratings.grouped(32).map(ratings => {
    val futures = ratings.map(postRating)
    Await.ready(Future.sequence(futures), 30.seconds)
  })


  responseFutures.foreach(println)

  system.terminate()
}
