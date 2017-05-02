package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.Future


case class ItemCount (
                     itemId: String,
                     count: Long
                     )


class ItemCountsTable extends CassandraTable[ItemCounts, ItemCount] {

  object itemId extends StringColumn(this) with PartitionKey
  object count extends CounterColumn(this)

  override def fromRow(row: Row): ItemCount = {
    ItemCount(
      itemId(row),
      count(row)
    )
  }
}

abstract class ItemCounts extends ItemCountsTable with RootConnector {

  def store(itemCount: ItemCount): Future[ResultSet] = {
    insert.value(_.itemId, itemCount.itemId).value(_.count, itemCount.count)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(id: String): Future[Option[ItemCount]] = {
    select.where(_.itemId eqs id).one()
  }

  def incrementCount(itemId: String, deltaWeight: Int): Future[ResultSet] = {
    update.where(_.itemId eqs itemId).modify(_.count += deltaWeight).future()
  }

}
