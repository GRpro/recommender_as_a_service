package gr.ml.analytics.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.RecommenderService
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.domain.JsonSerDeImplicits._

class RecommenderAPI(val ratingService: RecommenderService) {

  val route: Route =
    path("ratings") {
      post {
        entity(as[List[Map[String, Any]]]) { ratings =>
          ratings.foreach { map =>
//             TODO For Grisha - this should be an instance of Rating Service, not Recommendation service
            ratingService.save(map.get("userId").get.toString.toInt, map.get("itemId").get.toString.toInt, map.get("rating").get.toString.toDouble,
            map.get("timestamp").getOrElse[Any](System.currentTimeMillis()/1000).toString.toLong)
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
