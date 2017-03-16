package gr.ml.analytics.api

import akka.actor.{Actor, ActorRefFactory}
import gr.ml.analytics.service.RatingsService
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.{HttpService, _}


class RatingsAPI(val service: RatingsService) extends Actor with HttpService {

  implicit val routingSettings = RoutingSettings.default(context)

  override implicit def actorRefFactory: ActorRefFactory = context

  override def receive: Receive = runRoute(ratingsRoute)


  private def ratingsRoute = {
    respondWithMediaType(MediaTypes.`application/json`) {

      path("ratings") {
        post { // TODO shouldn't we provide parameters in request body for post?
          parameters('userId.as[Int], 'movieId.as[Int], 'rating.as[Double]) { (userId, movieId, rating) =>

            service.create(userId, movieId, rating)

            complete(StatusCodes.Created)
          }
        }
      } ~
        path("ratings") {
          get {
            parameters('userId.as[Int], 'pageSize.as[Int], 'page.as[Int]) { (userId, pageSize, page) =>
              complete {
                service.getTop(userId, pageSize, page)
              }
            }
          }
        }
    }
  }
}
