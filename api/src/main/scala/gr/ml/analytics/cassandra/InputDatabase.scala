package gr.ml.analytics.cassandra

import com.outworkers.phantom.database.Database
import com.outworkers.phantom.dsl.KeySpaceDef

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * The database which stores user input such as ratings and items.
  * Currently only ratings are supported.
  *
  * @param connector Cassandra connector
  */
class InputDatabase(override val connector: KeySpaceDef) extends Database[InputDatabase](connector) {

  object itemModel extends ConcreteItemModel with connector.Connector

  object ratingModel extends ConcreteRatingModel with connector.Connector

  // create tables if not exist
  Await.ready(itemModel.create.ifNotExists().future(), 3.seconds)
  Await.ready(ratingModel.create.ifNotExists().future(), 3.seconds)
}
