package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.Schema
import org.json4s.{DefaultFormats, Formats}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.parsing.json._

class SchemaServiceImpl(inputDatabase: InputDatabase) extends SchemaService with LazyLogging {

  private lazy val schemasModel = inputDatabase.schemasModel

  private var isSaved = false

  implicit val jsonFormats: Formats = DefaultFormats

  def normalize(jsonSchema: String): String = {
    val json = JSON.parseFull(jsonSchema)
    json match {
      case Some(schema: Map[String, Any]) => {
        require(schema.contains("id"))
        require(schema("id").asInstanceOf[Map[String, Any]].contains("name"))
        require(schema("id").asInstanceOf[Map[String, Any]].contains("type"))

        def toJson(m: Map[String, Any]): String = JSONObject(
          m.mapValues {
            case mp: Map[String, Any] => JSONObject(mp)
            case lm: List[Map[String, Any]] => JSONArray(lm.map(JSONObject))
            case x => x
          }
        ).toString

        toJson(schema)
      }
      case None => throw new RuntimeException("validation error")
    }

  }

  override def get(id: Int): Future[String] = {
    schemasModel.getOne(id).map {
      case Some(schema) => schema.jsonSchema
      case None => ""
    }
  }

  override def save(jsonSchema: String): Int = {
    if (isSaved) {
      throw new RuntimeException("Saving multiple schemas is not supported yet!")
    }

    val normalizedJsonSchema = normalize(jsonSchema)
    logger.info(s"Saving $normalizedJsonSchema")

    val id = 0

    schemasModel.save(Schema(id, normalizedJsonSchema))

    isSaved = true
    id
  }
}
