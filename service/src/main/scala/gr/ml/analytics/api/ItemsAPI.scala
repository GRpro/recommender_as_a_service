package gr.ml.analytics.api

import akka.actor.{Actor, ActorRefFactory}
import gr.ml.analytics.service.ItemService
import spray.http.MediaTypes
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.routing.ExceptionHandler._
import spray.routing.{HttpService, RoutingSettings}

class ItemsAPI(val service: ItemService) extends Actor with HttpService {

  implicit val routingSettings = RoutingSettings.default(context)

  override implicit def actorRefFactory: ActorRefFactory = context

  override def receive: Receive = runRoute(moviesRoute)


  private def moviesRoute = {
    respondWithMediaType(MediaTypes.`application/json`) {

      path("items" / IntNumber) { itemId =>
        get {
          complete {
            service.get(itemId)
          }
        }
      } ~
      path("items") {
        get {
          parameter('id.as[String]) { id =>
            complete {
              val ids: List[Int] = id.split(",").map(_.toInt).toList
              service.get(ids)
            }
          }
        }
      } ~
      path("items") {
        get {
          parameter('query.as[String]) { query =>
            complete {
              // TODO implement search by query
              ???
            }
          }
        }
      }
    }
  }
}
