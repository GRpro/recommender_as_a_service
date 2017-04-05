package gr.ml.analytics

import akka.actor.ActorSystem
import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.service.{Constants, Rating}
import spray.client.pipelining._
import spray.http._
import spray.httpx.SprayJsonSupport._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Command line tool used to download movielens dataset
  * to local file system and upload ratings from it to the recommender service
  * via REST client.
  *
  * See https://grouplens.org/datasets/movielens
  */
object DatasetUploader extends App with Constants {
  val config: Config = ConfigFactory.load("application.conf")

    //  Util.loadAndUnzip() TODO Grisha, do we need it here?

  val reader = CSVReader.open(ratingsPath)

  val ratings = reader.toStreamWithHeaders.flatMap(map => {
    for {
      userId <- map.get("userId")
      itemId <- map.get("movieId")   // TODO change it to itemId
      rating <- map.get("rating")
    } yield Rating(userId.toInt, itemId.toInt, rating.toDouble)
  }).toList


  implicit val system = ActorSystem()
  import system.dispatcher

  def setContentType(mediaType: MediaType)(r: HttpResponse): HttpResponse = {
    r.withEntity(HttpEntity(ContentType(mediaType), r.entity.data))
  }

  val pipeline: HttpRequest => Future[HttpResponse] = (
    sendReceive
    ~> setContentType(MediaTypes.`application/json`)
    )

  val url = s"${config.getString("service.recommender.rest")}/ratings"
  var response = pipeline(Post(url, ratings))
  Await.ready(response, 20 seconds)

  system.terminate()
}
