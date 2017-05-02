package gr.ml.analytics.cassandra

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.connectors.RootConnector
import com.outworkers.phantom.dsl._
import scala.concurrent.Future


case class Action(name: String, weight: Double)

class ActionsTable extends CassandraTable[Actions, Action] {

  override def tableName: String = "actions"

  object name extends StringColumn(this) with PartitionKey
  object weight extends DoubleColumn(this)

  override def fromRow(r: Row): Action = Action(name(r), weight(r))
}

abstract class Actions extends ActionsTable with RootConnector {
  def store(action: Action): Future[ResultSet] = {
    insert
      .value(_.name, action.name)
      .value(_.weight, action.weight)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .future()
  }

  def getById(name: String): Future[Option[Action]] = {
    select
      .where(_.name eqs name)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .one()
  }


  def getAll(limit: Int = 100): Future[List[Action]] = {
    select
      .limit(limit)
      .consistencyLevel_=(ConsistencyLevel.ALL)
      .fetch()
  }
}