package gr.ml.analytics.online.cassandra

import com.outworkers.phantom.dsl._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

case class ItemCount (
                     itemId: String,
                     count: Double
                     )


class ItemCountsTable extends CassandraTable[ItemCounts, ItemCount] {

  object itemId extends StringColumn(this) with PartitionKey
  object count extends DoubleColumn(this)

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

  def setCount(itemId: String, count: Double): Future[ResultSet] = {
    update.where(_.itemId eqs itemId).modify(_.count setTo count).future()
  }

  def incrementCount(itemId: String, deltaWeight: Double): Future[_] = {
    val f = getById(itemId)
    f.onSuccess {
      case Some(itemCount) =>
        update.where(_.itemId eqs itemId).modify(_.count setTo (itemCount.count + deltaWeight)).future()
      case None => store(ItemCount(itemId, deltaWeight))
    }
    f
  }

}
