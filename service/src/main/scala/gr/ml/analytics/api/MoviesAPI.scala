package gr.ml.analytics.api

import akka.actor.{Actor, ActorRefFactory}
import gr.ml.analytics.service.MoviesService
import spray.http.MediaTypes
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.ExceptionHandler._
import spray.routing.{HttpService, RoutingSettings}

class MoviesAPI(val service: MoviesService) extends Actor with HttpService {

  implicit val routingSettings = RoutingSettings.default(context)

  override implicit def actorRefFactory: ActorRefFactory = context

  override def receive: Receive = runRoute(moviesRoute)


  private def moviesRoute = {
    respondWithMediaType(MediaTypes.`application/json`) {

      path("movies" / LongNumber) { movieId =>
        get {
          complete {
            service.get(movieId)
          }
        }
      } ~
      path("movies") {
        get {
          parameter('id.as[String]) { id =>
            complete {
              val ids: List[Long] = id.split(",").map(_.toLong).toList
              service.get(ids)
            }
          }
        }
      }

    }
  }
}
