package gr.ml.analytics.cassandra


import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._

import scala.concurrent.Future

case class Rating(
                   userId: Int,
                   itemId: Int,
                   rating: Double,
                   timestamp: Long)

/**
  * Cassandra representation of the Ratings table
  */
class RatingModel extends CassandraTable[ConcreteRatingModel, Rating] {

  override def tableName: String = "ratings"

  object key extends StringColumn(this) with PartitionKey

  object userId extends IntColumn(this)

  object itemId extends IntColumn(this)

  object rating extends DoubleColumn(this)

  object timestamp extends LongColumn(this)

  override def fromRow(r: Row): Rating = Rating(userId(r), itemId(r), rating(r), timestamp(r))
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRatingModel extends RatingModel with RootConnector {

  def getAll: Future[List[Rating]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getAfter(userId: Int, timestamp: Long): Future[List[Rating]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .where(_.timestamp isGt timestamp)
      .fetch
  }

  def save(rating: Rating) = {
    insert
      .value(_.key, rating.userId + ":" + rating.itemId)
      .value(_.userId, rating.userId)
      .value(_.itemId, rating.itemId)
      .value(_.rating, rating.rating)
      .value(_.timestamp, rating.timestamp)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .future()
  }
}


