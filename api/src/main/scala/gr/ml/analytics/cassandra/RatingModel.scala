package gr.ml.analytics.cassandra

import java.util.UUID

import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl.{ConsistencyLevel, DoubleColumn, IntColumn, PartitionKey, RootConnector, Row, _}
import gr.ml.analytics.domain.Rating

import scala.concurrent.Future

/**
  * Cassandra representation of the Ratings table
  */
class RatingModel extends CassandraTable[ConcreteRatingModel, Rating] {

  override def tableName: String = "ratings"

  object id extends UUIDColumn(this) with ClusteringOrder

  object userId extends IntColumn(this)

  object itemId extends IntColumn(this)

  object rating extends DoubleColumn(this)

  object timestamp extends LongColumn(this) with PartitionKey

  override def fromRow(r: Row): Rating = Rating(userId(r), itemId(r), rating(r), timestamp(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRatingModel extends RatingModel with RootConnector { // TODO wouldnt it be better to rename to AbstractRatingModel? it's not concrete ;-)

  def getAll: Future[List[Rating]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def save(rating: Rating): UUID = {
    val id = UUID.randomUUID()
    insert
      .value(_.id, id)
      .value(_.userId, rating.userId)
      .value(_.itemId, rating.itemId)
      .value(_.rating, rating.rating)
      .value(_.timestamp, rating.timestamp)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
    id
  }

}


