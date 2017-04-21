package gr.ml.analytics.service

import scala.concurrent.Future
import scala.util.Try

/**
  * Schema service exposes interface to manage a schema of a particular set of items.
  */
trait SchemaService {

  def get(id: Int): Future[String]

  def save(jsonSchema: String): Int
}
