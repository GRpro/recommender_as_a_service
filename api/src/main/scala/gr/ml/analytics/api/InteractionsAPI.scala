package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.actor._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, path, post, _}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.cassandra.Action
import gr.ml.analytics.online.Interaction
import gr.ml.analytics.service.{ActionService, RatingService}
import io.swagger.annotations._
import spray.json.{JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.annotation.meta.field
import scala.concurrent.Future

@Api(value = "Ratings", produces = "application/json", consumes = "application/json")
@Path("/")
class InteractionsAPI(val ratingService: RatingService, val actionService: ActionService,  onlineModelActor: ActorRef) {

  val route: Route = postInteraction ~ postRatings

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "User interaction")
  case class InteractionView(
                              @(ApiModelProperty @field)(value = "Unique user identifier")
                              userId: Int,
                              @(ApiModelProperty @field)(value = "Unique item identifier")
                              itemId: Int,
                              @(ApiModelProperty @field)(value = "Name of user action")
                              action: String,
                              @(ApiModelProperty @field)(value = "Optional point of time when an interaction was performed. If not specified the timestamp is generated when this interaction is created", required = false)
                              timestamp: Option[Long])

  implicit val actionViewJsonFormat: RootJsonFormat[InteractionView] = jsonFormat4(InteractionView.apply)

  @ApiOperation(httpMethod = "POST", value = "Create interactions")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Interactions list", required = true, paramType = "body", collectionFormat = "List", dataType = "gr.ml.analytics.api.InteractionsAPI$InteractionView")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "All interactions created", responseContainer = "List"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Action not found for at least one interaction")))
  @Path("/interactions")
  def postInteraction: Route =
    path("interactions") {
      post {
        entity(as[List[InteractionView]]) { interactions =>
          onSuccess(Future.sequence(
            interactions.map(interaction =>
              actionService.getByName(interaction.action).map(actionOption => (interaction, actionOption))))) {
            case list: List[(InteractionView, Option[Action])] =>

              if (list.forall(_._2.isDefined)) {

                list.foreach(element => {
                  val action = element._2.get
                  val weight = action.weight
                  val interaction: Interaction = Interaction(
                    element._1.userId.toString,
                    element._1.itemId.toString,
                    action.name,
                    element._1.timestamp.getOrElse(System.currentTimeMillis() / 1000).toString.toLong)
                  onlineModelActor ! (interaction, weight)
                  })

                complete(StatusCodes.Created)
              } else {
                // action is not found for at least one interaction
                complete(StatusCodes.NotFound)
              }

            case _ =>
              complete(StatusCodes.InternalServerError)
          }
        }
      }
    }


  @ApiModel(description = "Rating object")
  case class Rating(
                     @(ApiModelProperty @field)(value = "unique identifier for the user")
                     userId: Int,
                     @(ApiModelProperty @field)(value = "unique identifier for the item")
                     itemId: Int,
                     @(ApiModelProperty @field)(value = "rating to describe how user prefer an item")
                     rating: Double,
                     @(ApiModelProperty @field)(value = "long timestamp when user rated item")
                     timestamp: Long)


  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
      case x => throw new RuntimeException
    }
    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => throw new RuntimeException
    }
  }

  // Deprecated. Not removed for backward compatibility
  @ApiOperation(httpMethod = "POST", value = "Create ratings")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Rating definition", required = true, paramType = "body", collectionFormat = "List", dataType = "gr.ml.analytics.api.InteractionsAPI$Rating")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Ratings have been created"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  @Path("/ratings")
  def postRatings: Route =
    path("ratings") {
      post {
        entity(as[List[Map[String, Any]]]) { ratings =>
          ratings.foreach { map =>
            ratingService.save(
              map("userId").toString.toInt,
              map("itemId").toString.toInt,
              map("rating").toString.toDouble,
              map.getOrElse("timestamp", System.currentTimeMillis() / 1000).toString.toLong
            )
          }

          complete(StatusCodes.Created)
        }
      }
    }
}
