package gr.ml.analytics.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, post}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.RatingService
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import gr.ml.analytics.domain.JsonSerDeImplicits._

class RatingsAPI(val ratingService: RatingService) {

  val route: Route =
    path("ratings") {
      post {
        entity(as[List[Map[String, Any]]]) { ratings =>
          ratings.foreach { map =>
            ratingService.save(
              map("userId").toString.toInt,
              map("itemId").toString.toInt,
              map("rating").toString.toDouble,
              map.getOrElse("timestamp", System.currentTimeMillis() / 1000).toString.toLong
            )
          }

          complete(StatusCodes.Created)
        }
      }
    }
}
