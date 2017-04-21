package gr.ml.analytics.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.RecommenderService
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.domain.JsonSerDeImplicits._

class RecommenderAPI(val recommenderService: RecommenderService) {

  val route: Route =
      path("recommendations") {
        get {
          parameters('userId.as[Int], 'top.as[Int]) { (userId, top) =>
            complete {
              recommenderService.getTop(userId, top)
            }
          }
        }
      }
}
