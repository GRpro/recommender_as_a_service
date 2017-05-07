package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl.KeySpaceDef
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class OnlineCassandraStorage(override val connector: KeySpaceDef) extends Database[OnlineCassandraStorage](connector) with LazyLogging {

  object users extends Users with connector.Connector
  object itemCounts extends ItemCounts with connector.Connector
  object pairCounts extends PairCounts with connector.Connector
  object similarities extends Similarities with connector.Connector
  object similaritiesIndex extends SimilaritiesIndex with connector.Connector

  try {
    Await.ready(createAsync(), 20.seconds)
  } catch {
    case e: Throwable =>
      //ignore
      logger.warn("Error creating models", e)
  }
}
