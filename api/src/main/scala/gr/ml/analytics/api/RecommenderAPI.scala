package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.RecommenderService
import io.swagger.annotations._
import spray.json.RootJsonFormat

import scala.annotation.meta.field

@Api(value = "Recommendations", produces = "application/json", consumes = "application/json")
@Path("/recommendations")
class RecommenderAPI(val recommenderService: RecommenderService) {

  val route: Route = getRecommendations

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "Recommendation object")
  case class RecommendationView(
                             @(ApiModelProperty @field)(value = "unique identifier of the user")
                             userId: Int,
                             @(ApiModelProperty @field)(value = "sorted list of recommended items represented by sorted list of item ids")
                             topItems: List[Int])

  implicit val recommendationViewJsonFormat: RootJsonFormat[RecommendationView] = jsonFormat2(RecommendationView.apply)

  @ApiOperation(httpMethod = "GET", response = classOf[RecommendationView], value = "Get recommendations for user")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "userId", required = true, dataType = "integer", paramType = "query", value = "id of the user to obtain recommendations for"),
    new ApiImplicitParam(name = "top", required = true, dataType = "integer", paramType = "query", value = "Number of items to be recommended in sorted order")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok", response = classOf[RecommendationView]),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "No recommendations for user found")))
  def getRecommendations: Route =
    path("recommendations") {
      get {
        parameters('userId.as[Int], 'top.as[Int]) { (userId, top) =>
          onSuccess(recommenderService.getTop(userId, top)) {
            case list: List[Int] => complete(RecommendationView(userId, list))
            case Nil => complete(StatusCodes.NotFound)
          }
        }
      }
    }
}
