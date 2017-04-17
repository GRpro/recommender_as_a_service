package gr.ml.analytics.api

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.domain.JsonSerDeImplicits._
import gr.ml.analytics.service.ItemService
import spray.json.DefaultJsonProtocol._

/*

[
  {
    "movieId": 1,
    "title": "Toy Story (1995)",
    "genres": "Adventure|Animation|Children|Comedy|Fantasy"
  }
]

 */
class ItemsAPI(val itemService: ItemService) {

  val route: Route =
    path("schemas" / IntNumber / "items") { schemaId =>
      post {
        entity(as[List[Map[String, Any]]]) { items =>
          items.foreach { item =>
            itemService.save(schemaId, item)
          }
          complete(StatusCodes.Created)
        }
      }
    } ~
      path("schemas" / IntNumber / "items" / IntNumber) { (schemaId: Int, itemId: Int) =>
        get {
          complete {
            itemService.get(schemaId, itemId)
          }
        }
      }
}
