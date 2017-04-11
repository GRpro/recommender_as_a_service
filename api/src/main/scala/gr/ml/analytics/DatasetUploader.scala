package gr.ml.analytics

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.domain.JsonSerDeImplicits._
import gr.ml.analytics.domain.{Item, Rating}
import gr.ml.analytics.util.Util

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


/**
  * Command line tool used to download movielens dataset
  * to local file system and upload ratings from it to the recommender service
  * via REST client.
  *
  * See https://grouplens.org/datasets/movielens
  */
object DatasetUploader extends App with Constants with LazyLogging {

  Util.loadAndUnzip()

  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()

  val config: Config = ConfigFactory.load("application.conf")


  def uploadRatings(): Unit = {

    val reader = CSVReader.open(ratingsPath)

    val uri = s"${config.getString("service.recommender.rest")}/ratings"

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

  }

  def uploadItems(): Unit = {

    val reader = CSVReader.open(moviesPath)

    val uri = s"${config.getString("service.items.rest")}/items"

    def postItem(item: Item): Future[HttpResponse] = {
      val future = Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = uri,
        // TODO fix this UGLY thing related to marshalling. Send a batch of entities per request.
        entity = HttpEntity(ContentTypes.`application/json`, "[" + itemFormat.write(item).toString() + "]")))

      future.onFailure{case e => e.printStackTrace()}
      future
    }

    val items = reader.toStreamWithHeaders.flatMap(map => {
      for {
        itemId <- map.get("movieId")
      } yield Item(itemId.toInt)
    }).toList

    val responseFutures = items.grouped(32).map(ratings => {
      val futures = ratings.map(postItem)
      Await.ready(Future.sequence(futures), 30.seconds)
    })

    responseFutures.foreach(println)
  }

  logger.info("uploading items")
  uploadItems()

  logger.info("Uploading ratings")
  uploadRatings()

  system.terminate()
}
