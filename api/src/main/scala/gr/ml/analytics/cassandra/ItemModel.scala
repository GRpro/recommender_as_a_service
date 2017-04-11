package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl.{ConsistencyLevel, IntColumn, PartitionKey, RootConnector, Row, _}
import gr.ml.analytics.domain.Item

import scala.concurrent.Future

/**
  * Cassandra representation of the Items table
  */
class ItemModel extends CassandraTable[ConcreteItemModel, Item] {

  override def tableName: String = "items"


  object itemId extends IntColumn(this) with PartitionKey

  // TODO store item features

  override def fromRow(r: Row): Item = Item(itemId(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteItemModel extends ItemModel with RootConnector {

  def getAll: Future[List[Item]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def save(item: Item): Int = {
    insert
      .value(_.itemId, item.itemId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
    item.itemId
  }

}
