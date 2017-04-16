package gr.ml.analytics.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.domain.Rating
import gr.ml.analytics.service.RecommenderService
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.domain.JsonSerDeImplicits._

class RecommenderAPI(val ratingService: RecommenderService) {

  val route: Route =
    path("ratings") {
      post {
        entity(as[List[Rating]]) { ratings =>
          ratings.foreach { rating =>
            if (rating.timestamp == null)
              ratingService.save(rating.userId, rating.itemId, rating.rating, System.currentTimeMillis()) // TODO For Grisha - this should be an instance of Rating Service, not Recommendation service
            else
              ratingService.save(rating.userId, rating.itemId, rating.rating, rating.timestamp)
          }

          complete(StatusCodes.Created)
        }
      }
    } ~
      path("recommendations") {
        get {
          parameters('userId.as[Int], 'top.as[Int]) { (userId, top) =>
            complete {
              ratingService.getTop(userId, top)
            }
          }
        }
      }
}
