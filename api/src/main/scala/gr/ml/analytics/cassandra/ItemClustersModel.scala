package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.ClusteredItems

import scala.concurrent.Future


/**
  * Cassandra representation of the Item Clusters table
  */
class ClusteredItemsModel extends CassandraTable[ConcreteClusteredItemsModel, ClusteredItems] {

  override def tableName: String = "item_clusters"

  object itemid extends IntColumn(this) with PartitionKey

  object similar_items extends StringColumn(this)

  override def fromRow(r: Row): ClusteredItems = ClusteredItems(
    itemid(r),
    similar_items(r).split(":").map(itemId => itemId.toInt).toSet
  )
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteClusteredItemsModel extends ClusteredItemsModel with RootConnector {

  def getAll: Future[List[ClusteredItems]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getOne(itemId: Int): Future[Option[ClusteredItems]] = {
    select
      .where(_.itemid eqs itemId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .one
  }

}
