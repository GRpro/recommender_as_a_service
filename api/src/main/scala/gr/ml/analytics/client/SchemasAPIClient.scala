package gr.ml.analytics.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SchemasAPIClient(schemasBaseURI: String)(implicit val actorSystem: ActorSystem, implicit val actorMaterializer: Materializer) {

  def get(schemaId: Int): String = {
    val future: Future[HttpResponse] = Http().singleRequest(HttpRequest(
      method = HttpMethods.GET,
      uri = s"$schemasBaseURI/schemas/$schemaId"
    ))

    Await.ready(future, 1.second)
    future.value.get.get.entity.asInstanceOf[HttpEntity.Strict].getData().decodeString(ByteString.UTF_8)
  }

}
