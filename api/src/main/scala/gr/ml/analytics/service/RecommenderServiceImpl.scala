package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.Rating
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

class RecommenderServiceImpl(inputDatabase: InputDatabase) extends RecommenderService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel
  private lazy val recommendationModel = inputDatabase.recommendationsModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Long): Unit = {
    val ratingEntity = Rating(userId, itemId, rating, timestamp)
    ratingModel.save(ratingEntity)

    logger.info(s"saved $ratingEntity")

  }

  /**
    * @inheritdoc
    */
  override def getTop(userId: Int, n: Int): Future[List[Int]] = {
    recommendationModel.getOne(userId).map {
      case Some(recommendation) => recommendation.topItems.take(n)
      case None => Nil
    }
  }
}
