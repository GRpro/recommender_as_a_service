package gr.ml.analytics.cassandra


import com.outworkers.phantom.CassandraTable
import com.outworkers.phantom.dsl._
import gr.ml.analytics.domain.Recommendation

import scala.concurrent.Future

/**
  * Cassandra representation of the Ratings table
  */
class RecommendationModel extends CassandraTable[ConcreteRecommendationModel, Recommendation] {

  override def tableName: String = "final_recommendations"

  object userid extends IntColumn(this) with PartitionKey

  object recommended_ids extends StringColumn(this)

  override def fromRow(r: Row): Recommendation = Recommendation(
    userid(r),
    recommended_ids(r).split(":").map(itemId => itemId.toInt).toList
  )
}

/**
  * Define the available methods for this model
  */
abstract class ConcreteRecommendationModel extends RecommendationModel with RootConnector {

  def getAll: Future[List[Recommendation]] = {
    select
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .fetch
  }

  def getOne(userId: Int): Future[Option[Recommendation]] = {
    select
      .where(_.userid eqs userId)
      .consistencyLevel_=(ConsistencyLevel.ONE)
      .one
  }

}
