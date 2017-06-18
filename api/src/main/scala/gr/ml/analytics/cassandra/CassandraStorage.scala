package gr.ml.analytics.cassandra

import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl.KeySpaceDef
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class CassandraStorage(override val connector: KeySpaceDef) extends Database[CassandraStorage](connector) with LazyLogging {

  object recommendationsModel extends ConcreteRecommendationModel with connector.Connector
  object ratingModel extends ConcreteRatingModel with connector.Connector
  object schemasModel extends ConcreteSchemaModel with connector.Connector
  object clusteredItemsModel extends ConcreteClusteredItemsModel with connector.Connector
  object actionsModel extends Actions with connector.Connector

  try {
    Await.ready(createAsync(), 20.seconds)
  } catch {
    case e: Throwable =>
      //ignore
      logger.warn("Error creating models", e)
  }

}
