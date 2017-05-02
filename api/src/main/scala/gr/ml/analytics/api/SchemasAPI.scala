package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.SchemaService
import io.swagger.annotations._
import spray.json.{JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat}

import scala.annotation.meta.field
import scala.util.{Failure, Success, Try}

/*

Example schema:

{
	"id": {
		"name": "movieId",
		"type": "Int"
	},
	"features": [
	  {
		  "name": "title",
		  "type": "text"
	  },
	  {
		  "name": "genres",
		  "type": "text"
	  }
	]
}

 */
@Api(value = "Schemas", produces = "application/json", consumes = "application/json")
@Path("/schemas")
class SchemasAPI(schemaService: SchemaService) {

  val route: Route = postSchema ~ getSchema

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import spray.json.DefaultJsonProtocol._

  @ApiModel(description = "Schema object")
  case class SchemaView(
                     @(ApiModelProperty @field)(value = "unique identifier for the schema")
                     schemaId: Int,
                     @(ApiModelProperty @field)(value = "json definition of the schema")
                     jsonSchema: Map[String, Any])

  implicit val schemaFormat: RootJsonFormat[SchemaView] = jsonFormat2(SchemaView.apply)

//  implicit val anyJsonFormat: RootJsonFormat[Map[String, Any]] = AnyJsonFormat

  implicit val anyFormat: JsonFormat[Any] = AnyJsonFormat
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

  @ApiOperation(httpMethod = "POST", value = "Create schema")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Schema definition", required = true, paramType = "body", dataType = "gr.ml.analytics.api.SchemasAPI$SchemaView")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 201, message = "Schema has been created"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 500, message = "Internal server error")))
  def postSchema: Route =
    path("schemas") {
      post {
        entity(as[Map[String, Any]]) { schema =>
          Try {
            schemaService.save(schema)
          } match {
            case Success(schemaId) => complete(StatusCodes.Created, schemaId.toString)
            case Failure(ex) => complete(StatusCodes.InternalServerError, s"Not created: ${ex.getMessage}")
          }

        }
      }
    }

  @ApiOperation(httpMethod = "GET", response = classOf[SchemaView], value = "Get schema by id")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "schemaId", required = true, dataType = "integer", paramType = "path", value = "schema id")
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "Ok"),
    new ApiResponse(code = 400, message = "Bad request"),
    new ApiResponse(code = 404, message = "Schema not found")))
  @Path("/{schemaId}")
  def getSchema: Route =
    path("schemas" / IntNumber) { schemaId =>
      get {
        onSuccess(schemaService.get(schemaId)) {

          case Some(schema: SchemaView) =>
            complete(StatusCodes.OK, schema.jsonSchema)
          case None => complete(StatusCodes.NotFound)
        }
      }
    }
}
