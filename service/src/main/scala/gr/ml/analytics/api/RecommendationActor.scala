package gr.ml.analytics.api

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.Start
import gr.ml.analytics.model.MovieRecommendationServiceImpl
import gr.ml.analytics.movies._

class RecommendationActor extends Actor with LazyLogging {

  lazy val service: MovieRecommendationService = MovieRecommendationServiceImpl()

  override def receive: Receive = {
    case Start => context.become(predictionsState)
      logger.info("Recommendation service initialized")
    case _ =>
      logger.warn("Service is not ready to make recommendations")
  }

  def predictionsState: Receive = {
    case ItemsToBeRatedRequest(n) =>
      ItemsToBeRated(service.getItems(n))

    case RateItems(ratedItems) =>
      logger.info(s"Rate ${ratedItems.size} items")
      service.rateItems(ratedItems)

    case TopNMoviesForUserRequest(user, n) =>
      TopNMoviesForUser(
        service.getTopNForUser(user, n)
        .map(itemRating => (user, itemRating._1, itemRating._2)))
  }
}
