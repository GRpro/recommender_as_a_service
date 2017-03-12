package gr.ml.analytics.model

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.entities.{Movie, MovieRecommendationService, Rating, User}

object MovieRecommendationServiceImpl {

  def apply(dataStore: DataStore): MovieRecommendationServiceImpl = new MovieRecommendationServiceImpl(dataStore)

  def apply(): MovieRecommendationServiceImpl = {
    val sparkSession = SparkUtil.sparkSession()
    val dataStore = DataStore(sparkSession)
    apply(dataStore)
  }
}

class MovieRecommendationServiceImpl(var dataStore: DataStore) extends MovieRecommendationService with LazyLogging {
  var model: Option[PredictionModel] = None
  override def getItems(n: Int): List[Movie] = {
    dataStore.movies(n)
  }

  override def rateItems(rated: List[(User, Movie, Rating)]): Unit = {
    dataStore = dataStore.rate(rated)
    model = Some(dataStore.trainModel.buildPredictionModel(dataStore))
  }

  override def getTopNForUser(user: User, n: Int): List[(Movie, Rating)] = {
    model match {
      case Some(m) =>
        m.getTopNForUser(user, n)
      case None =>
        model = Some(dataStore.trainModel.buildPredictionModel(dataStore))
        List()
    }

  }

}
