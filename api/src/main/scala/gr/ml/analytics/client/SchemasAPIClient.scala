package gr.ml.analytics.client

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.Materializer
import akka.util.ByteString

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SchemasAPIClient(schemasBaseURI: String)(implicit val actorSystem: ActorSystem, implicit val actorMaterializer: Materializer) {

  private var cache: Map[Int, String] = Map()

  def get(schemaId: Int): String = {
    cache.get(schemaId) match {
      case Some(schema) => schema
      case None =>
        val future: Future[HttpResponse] = Http().singleRequest(HttpRequest(
          method = HttpMethods.GET,
          uri = s"$schemasBaseURI/schemas/$schemaId"
        ))

        Await.ready(future, 1.second)

        val response = future.value.get.get
        val schema = future.value.get.get.entity.asInstanceOf[HttpEntity.Strict].getData().decodeString(ByteString.UTF_8)
        cache = cache + (schemaId -> schema)
        schema
    }
  }
}
