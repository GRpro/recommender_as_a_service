package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.Rating

class RatingServiceImpl(inputDatabase: InputDatabase) extends RatingService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Long): Unit = {
    val ratingEntity = Rating(userId, itemId, rating, timestamp)
    ratingModel.save(ratingEntity)

    logger.info(s"saved $ratingEntity")
  }

}
