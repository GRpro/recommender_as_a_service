package gr.ml.analytics.entities

import gr.ml.analytics.RecommendationServiceAPI
import spray.json.DefaultJsonProtocol

case class User(userId: Long)

object User extends DefaultJsonProtocol {
  implicit val userFormat = jsonFormat1(User.apply)
}


case class Movie(movieId: Long, title: String, genres: String, imdbId: String, tmdbId: String)

object Movie extends DefaultJsonProtocol {
  implicit val movieFormat = jsonFormat5(Movie.apply)
}

// TODO rename to Rating after current class 'Rating' is removed
case class MovieRating(movieId: Long, rating: Double) {
  if (rating < 0 || rating > 5)
    throw new IllegalArgumentException(
      "Movie rating is double value between 0 and 5")
}

object MovieRating extends DefaultJsonProtocol {
  implicit val movieRatingFormat = jsonFormat2(MovieRating.apply)
}

// TODO Old entities below - remove them

case class Rating(rating: Double, timestamp: String) {

  if (rating < 0 || rating > 5)
    throw new IllegalArgumentException(
      "Movie rating is double value between 0 and 5")
}

object Rating extends DefaultJsonProtocol {
  implicit val ratingFormat = jsonFormat2(Rating.apply)
}


case class UserMovieRating(user: User, movie: Movie, rating: Rating)

object UserMovieRating extends DefaultJsonProtocol {
  implicit val userMovieRatingFormat = jsonFormat3(UserMovieRating.apply)
}



trait MovieRecommendationServiceAPI extends RecommendationServiceAPI[User, Movie, Rating]
