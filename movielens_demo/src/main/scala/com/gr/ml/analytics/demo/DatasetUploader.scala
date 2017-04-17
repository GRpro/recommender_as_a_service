package com.gr.ml.analytics.demo

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.ByteString
import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.parsing.json.{JSONArray, JSONObject}
import scala.util.{Failure, Success}


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

  val schemasREST = config.getString("service.schemas.rest")
  val itemsREST = config.getString("service.items.rest")
  val ratingsREST = config.getString("service.ratings.rest")

  /**
    * Creates schema for item format.
    *
    * @return the id of created schema
    */
  def postSchema(): Int = {
    val future = Http().singleRequest(HttpRequest(
      method = HttpMethods.POST,
      uri = s"$schemasREST/schemas",
      // TODO fix this UGLY thing related to marshalling. Send a batch of entities per request.
      entity = HttpEntity(ContentTypes.`application/json`,
        """
          |{
          |	"id": {
          |		"name": "movieId",
          |		"type": "Int"
          |	},
          |	"features": [
          |	  {
          |		  "name": "title",
          |		  "type": "text"
          |	  },
          |	  {
          |		  "name": "genres",
          |		  "type": "text"
          |	  }
          |	]
          |}
        """.stripMargin)))

    future.onFailure { case e => e.printStackTrace() }
    Await.ready(future, 10.seconds)
    future.value.get match {
      case Success(response) =>
        // TODO more elegant way to get Int value from response?
        response.entity.asInstanceOf[HttpEntity.Strict].getData().decodeString(ByteString.UTF_8).toInt
      case Failure(ex) => throw ex
    }
  }

  case class Movie(movieId: Int, title: String, genres: String)

  def uploadMovies(schemaId: Int): Unit = {

    val reader = CSVReader.open(moviesPath)

    val allItems = reader.toStreamWithHeaders.flatMap(map => {
      for {
        movieId <- map.get("movieId")
        title <- map.get("title")
        genres <- map.get("genres")
      } yield Movie(movieId.toInt, title.toString, genres.toString)
    }).toList




    // TODO BUG - not all movies are being uploaded, only 9025
    def postItem(movieList: List[Movie]): Future[HttpResponse] = {

      def toJson(movieList: List[Movie]): String = {
        JSONArray(movieList.map(movie => JSONObject(Map(
          "movieId" -> movie.movieId,
          "title" -> movie.title,
          "genres" -> movie.genres)))).toString()
      }

      val future = Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = s"$itemsREST/schemas/$schemaId/items",
        entity = HttpEntity(ContentTypes.`application/json`, toJson(movieList))))

      future.onFailure { case e => e.printStackTrace() }
      future
    }

    // every request will contain 1000 movies
    val groupedMovies: List[List[Movie]] = allItems.grouped(1000).toList

    groupedMovies.foreach(movies => {
      val future = postItem(movies)
      Await.ready(future, 30.seconds)
      println(future.value.get)
    })
  }

  case class Rating(userId: Int, itemId: Int, rating: Double)

  def uploadRatings(): Unit = {

    val reader = CSVReader.open(ratingsPath)

    def postRating(ratingList: List[Rating]): Future[HttpResponse] = {

      def toJson(ratingList: List[Rating]): String = {
        JSONArray(ratingList.map(rating => JSONObject(Map(
          "userId" -> rating.userId,
          "itemId" -> rating.itemId,
          "rating" -> rating.rating)))).toString()
      }

      val future = Http().singleRequest(HttpRequest(
        method = HttpMethods.POST,
        uri = s"$ratingsREST/ratings",
        entity = HttpEntity(ContentTypes.`application/json`, toJson(ratingList))))

      future.onFailure { case e => e.printStackTrace() }
      future
    }

    val allRatings = reader.toStreamWithHeaders.flatMap(map => {
      for {
        userId <- map.get("userId")
        itemId <- map.get("movieId")
        rating <- map.get("rating")
      } yield Rating(userId.toInt, itemId.toInt, rating.toDouble)
    }).toList

    // every request will contain 1000 ratings
    val groupedMovies: List[List[Rating]] = allRatings.grouped(1000).toList

    groupedMovies.foreach(ratings => {
      val future = postRating(ratings)
      Await.ready(future, 30.seconds)
      println(future.value.get)
    })

  }

  logger.info("Creating schema")
  val schemaId = postSchema()

  logger.info("Uploading items corresponding to schema")
  uploadMovies(schemaId)

  logger.info("Uploading ratings")
  uploadRatings()

  system.terminate()
}
