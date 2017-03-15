package gr.ml.analytics.api

import akka.actor.{Actor, ActorRefFactory}
import gr.ml.analytics.entities.MovieRating
import gr.ml.analytics.service.RatingsService
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.routing.{HttpService, _}


class RatingsAPI(val service: RatingsService) extends Actor with HttpService {

  implicit val routingSettings = RoutingSettings.default(context)

  override implicit def actorRefFactory: ActorRefFactory = context

  override def receive: Receive = runRoute(ratingsRoute)


  private def ratingsRoute = {
    respondWithMediaType(MediaTypes.`application/json`) {

      path("ratings") {
        post {
          entity(as[List[MovieRating]]) { ratings =>
            parameter('userId.as[Long]) { userId =>

              service.create(userId, ratings)

              complete(StatusCodes.Created)
            }
          }
        }
      } ~
        path("ratings") {
          get {
            parameters('userId.as[Long], 'pageSize.as[Int], 'page.as[Int]) { (userId, pageSize, page) =>
              complete {
                service.getTop(userId, pageSize, page)
              }
            }
          }
        }
    }
  }
}
