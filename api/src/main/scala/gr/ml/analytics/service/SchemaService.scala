package gr.ml.analytics.service

import gr.ml.analytics.cassandra.Schema

import scala.concurrent.Future

/**
  * Schema service exposes interface to manage a schema of a particular set of items.
  */
trait SchemaService {

  def get(id: Int): Future[Option[Schema]]

  def save(jsonSchema: Map[String, Any]): Int
}
