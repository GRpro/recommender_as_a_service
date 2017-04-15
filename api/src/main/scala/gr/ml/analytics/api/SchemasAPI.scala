package gr.ml.analytics.api

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives.{as, complete, entity, get, path, post}
import akka.http.scaladsl.server.Route
import gr.ml.analytics.service.SchemaService
import akka.http.scaladsl.server.Directives._

import scala.util.{Failure, Success, Try}

/*

Example schema:

{
  [
    "id": {
      "name": "movieId",
      "type": "Int"
    },
    "features": [
      {
        "name": "title",
        "type": "String"
      },
      {
        "name": "genres",
        "type": "String"
      }
    ]
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
        get {
          complete {
            schemaService.get(schemaId)
          }
        }
      }
}
