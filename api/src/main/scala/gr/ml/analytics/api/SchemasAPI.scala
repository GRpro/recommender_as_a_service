package gr.ml.analytics.api

import javax.ws.rs.Path

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import gr.ml.analytics.domain.Schema
import gr.ml.analytics.service.SchemaService
import io.swagger.annotations._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import gr.ml.analytics.domain.JsonSerDeImplicits._

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


  @ApiOperation(httpMethod = "POST", value = "Create schema")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "body", value = "Schema definition", required = true, paramType = "body")
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

  @ApiOperation(httpMethod = "GET", response = classOf[Schema], value = "Get schema by id")
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

          case Some(schema: Schema) =>
            complete(StatusCodes.OK, schema.jsonSchema)
          case None => complete(StatusCodes.NotFound)
        }
      }
    }
}
