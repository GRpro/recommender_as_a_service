package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class RecommenderServiceImpl(inputDatabase: InputDatabase) extends RecommenderService with LazyLogging {

  private lazy val recommendationModel = inputDatabase.recommendationsModel
  private lazy val clusteredItemsModel = inputDatabase.clusteredItemsModel
  private lazy val ratingsModel = inputDatabase.ratingModel

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
