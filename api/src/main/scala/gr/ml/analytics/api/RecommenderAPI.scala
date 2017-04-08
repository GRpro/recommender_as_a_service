package gr.ml.analytics.api

import akka.actor.{Actor, ActorRefFactory}
import gr.ml.analytics.service.{Rating, RecommenderService}
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.{HttpService, _}

class RecommenderAPI(val ratingService: RecommenderService) extends Actor with HttpService {

  implicit val routingSettings = RoutingSettings.default(context)

  override implicit def actorRefFactory: ActorRefFactory = context

  override def receive: Receive = runRoute(ratingsRoute)


  private def ratingsRoute = {
    respondWithMediaType(MediaTypes.`application/json`) {

      path("ratings") {
        post {
          entity(as[List[Rating]]) { ratings =>
            ratings.foreach { rating =>
              ratingService.save(rating.userId, rating.itemId, rating.rating) // TODO For Grisha - this should be an instance of Rating Service, not Recommendation service
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
  }
}
