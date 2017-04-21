package gr.ml.analytics.api

import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentTypes, HttpHeader, StatusCodes}
import akka.http.scaladsl.server.Directives.{as, complete, entity, get, path, post}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.SchemaService
import akka.http.scaladsl.server.Directives._

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
class SchemasAPI(schemaService: SchemaService) {

  val route: Route =
    path("schemas") {
      post {
        entity(as[String]) { schema =>
          Try {schemaService.save(schema)} match {
            case Success(schemaId) => complete(StatusCodes.Created, schemaId.toString)
            case Failure(ex) => complete(StatusCodes.NotImplemented, s"Not created: ${ex.getMessage}")
          }

        }
      }
    } ~
      path("schemas" / IntNumber) { schemaId =>
        respondWithHeader(RawHeader("Content-type", "application/json")) {
          get {
            complete {
              schemaService.get(schemaId)
            }
          }
        }
      }
}
