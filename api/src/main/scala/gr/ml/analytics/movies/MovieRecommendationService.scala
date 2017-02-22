package gr.ml.analytics.movies

import gr.ml.analytics.RecommendationService


case class User(id: Long)

case class Movie(id: Long, title: String, genres: String, imdbId: String, tmdbId: String)

case class Rating(rating: Double, timestamp: String) {

  if (rating < 0 || rating > 5)
    throw new IllegalArgumentException(
      "Movie rating is double value between 0 and 5")
}

trait MovieRecommendationService extends RecommendationService[User, Movie, Rating]

case class ItemsToBeRatedRequest(n: Int)

case class ItemsToBeRated(items: List[Movie])

case class RateItems(ratedItems: List[(User, Movie, Rating)])

case class TopNMoviesForUserRequest(user: User, n: Int)

case class TopNMoviesForUser(topN: List[(User, Movie, Rating)])