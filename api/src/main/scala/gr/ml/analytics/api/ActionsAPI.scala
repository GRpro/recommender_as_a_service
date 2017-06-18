package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.cassandra.Action
import gr.ml.analytics.service.ActionService
import io.swagger.annotations._
import spray.json.RootJsonFormat
import scala.concurrent.ExecutionContext.Implicits.global

import scala.annotation.meta.field


@Api(value = "Actions", produces = "application/json", consumes = "application/json")
@Path("/actions")
class ActionsAPI(val actionService: ActionService) {

  val route: Route = createAction ~ getAction ~ getActions

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "User action")
  case class ActionView(
                         @(ApiModelProperty @field)(value = "Unique name of user action")
                         name: String,
                         @(ApiModelProperty @field)(value = "Weight of user action. The higher weight means the higher likelihood the user likes an item")
                         weight: Double
                       )

  implicit val actionViewJsonFormat: RootJsonFormat[ActionView] = jsonFormat2(ActionView.apply)

  @ApiOperation(httpMethod = "GET", response = classOf[ActionView], value = "Get action by name")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "actionName", required = true, dataType = "string", paramType = "path", value = "Action name")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Action not found")))
  @Path("/{actionName}")
  def getAction: Route =
    path("actions" / ".*".r) { name =>
      get {
        onSuccess(actionService.getByName(name)) {
          case Some(action) =>
            complete(StatusCodes.OK, ActionView(action.name, action.weight))
          case None =>
            complete(StatusCodes.NotFound)
        }
      }
      }

  @ApiOperation(httpMethod = "GET", response = classOf[ActionView], value = "Get all actions")
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "No actions found")))
  def getActions: Route =
    path("actions") {
      get {
        onSuccess(actionService.getAll()) {
          case Nil =>
            complete(StatusCodes.NotFound)
          case actions: List[Action] =>
            complete(StatusCodes.OK, actions.map(action => ActionView(action.name, action.weight)))
        }
      }
    }

  @ApiOperation(httpMethod = "POST", value = "Add new action")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Action definition", required = true, paramType = "body", dataType = "gr.ml.analytics.api.ActionsAPI$ActionView")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Action created", response = classOf[String]),
    new ApiResponse(code = 400, message = "Bad request")))
  def createAction: Route =
    path("actions") {
      post {
        entity(as[ActionView]) { action =>
          onSuccess(actionService.create(Action(action.name, action.weight))) {
            case Some(action) =>
              complete(StatusCodes.Created)
            case None =>
              complete(StatusCodes.Conflict)
          }
        }
      }
    }

}
