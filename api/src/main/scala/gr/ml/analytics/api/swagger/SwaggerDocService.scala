package gr.ml.analytics.api.swagger

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.github.swagger.akka._
import com.github.swagger.akka.model.Info
import gr.ml.analytics.api.{ItemsAPI, RatingsAPI, RecommenderAPI, SchemasAPI}
import io.swagger.models.ExternalDocs
import io.swagger.models.auth.BasicAuthDefinition

import scala.reflect.runtime.{universe => ru}

class SwaggerDocService(val interface: String,
                        val port: Int)
                       (implicit val system: ActorSystem,
                        implicit val actorMaterializer: ActorMaterializer) extends SwaggerHttpService with HasActorSystem {

  override implicit val actorSystem: ActorSystem = system
  override implicit val materializer: ActorMaterializer = actorMaterializer

  override val apiTypes = Seq(
    ru.typeOf[ItemsAPI],
    ru.typeOf[RatingsAPI],
    ru.typeOf[RecommenderAPI],
    ru.typeOf[SchemasAPI]
  )
  override val host = s"$interface:$port"
  override val info = Info(version = "1.0")
  override val externalDocs = Some(new ExternalDocs("Core Docs", "http://acme.com/docs"))
  override val securitySchemeDefinitions = Map("basicAuth" -> new BasicAuthDefinition())
}