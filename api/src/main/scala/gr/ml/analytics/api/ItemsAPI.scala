package gr.ml.analytics.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.ItemService
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.service.JsonSerDeImplicits._

class ItemsAPI(val itemService: ItemService) {

  val route: Route =
    path("items" / IntNumber) { itemId =>
      get {
        complete {
          itemService.get(itemId)
        }
      }
    } ~
      path("items") {
        get {
          parameter('id.as[String]) { id =>
            complete {
              val ids: List[Int] = id.split(",").map(_.toInt).toList
              itemService.get(ids)
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
