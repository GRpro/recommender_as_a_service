package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.domain.Item
import gr.ml.analytics.domain.JsonSerDeImplicits._
import gr.ml.analytics.service.ItemService
import io.swagger.annotations._
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
@Api(value = "Items", produces = "application/json", consumes = "application/json")
@Path("/schemas/{schemaId}/items")
class ItemsAPI(val itemService: ItemService) {

  val route: Route = postItems ~ getItem

  @ApiOperation(httpMethod = "POST", value = "Add new item")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "integer", paramType = "path", value = "Schema identifier of an item"),
    new ApiImplicitParam(name = "body", value = "Item definition", required = true, paramType = "body")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Item has been created"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  def postItems: Route =
    path("schemas" / IntNumber / "items") { schemaId =>
      post {
        entity(as[List[Item]]) { items =>
          items.foreach { item =>
            itemService.save(schemaId, item)
          }
          complete(StatusCodes.Created)
        }
      }
    }

  @ApiOperation(httpMethod = "GET", response = classOf[Item], value = "Get item by id")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "integer", paramType = "path", value = "Schema identifier of an item"),
    new ApiImplicitParam(name = "itemId", required = true, dataType = "integer", paramType = "path", value = "Item identifier")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Item or schema not found")))
  @Path("/{itemId}")
  def getItem: Route =
    path("schemas" / IntNumber / "items" / IntNumber) { (schemaId: Int, itemId: Int) =>
      get {
        onSuccess(itemService.get(schemaId, itemId)) {
          case Some(item: Item) => complete(item)
          case None => complete(StatusCodes.NotFound, "Schema or item has not been found")
        }
      }
    }
}
