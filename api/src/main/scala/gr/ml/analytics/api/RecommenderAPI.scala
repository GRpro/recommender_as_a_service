package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.domain.Item
import gr.ml.analytics.service.RecommenderService
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.domain.JsonSerDeImplicits._
import io.swagger.annotations._

@Api(value = "Recommendations", produces = "application/json", consumes = "application/json")
@Path("/recommendations")
class RecommenderAPI(val recommenderService: RecommenderService) {

  val route: Route = getRecommendations

  @ApiOperation(httpMethod = "GET", response = classOf[Item], value = "Get recommendations for user")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "userId", required = true, dataType = "integer", paramType = "query", value = "id of the user to obtain recommendations for"),
    new ApiImplicitParam(name = "top", required = true, dataType = "integer", paramType = "query", value = "Number of items to be recommended in sorted order")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "No recommendations for user found")))
  def getRecommendations: Route =
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
