package gr.ml.analytics

import gr.ml.analytics.entities.{Movie, Rating, User}
import gr.ml.analytics.model.MovieRecommendationServiceImpl

import scala.io.StdIn

/**
  * Console application to predict movies for you :)
  */
object ConsoleApplication {

  def main(args: Array[String]): Unit = {

    val movieRecommendationModelService = MovieRecommendationServiceImpl()

    val toBeRated: List[Movie] = movieRecommendationModelService.getItems(10)
    println("Please rate the following movies [0-5]")
    val rated = toBeRated.map(movie => {
      println(s"${movie.movieId}, ${movie.title} [0-5]")
      val rating = StdIn.readDouble()
      (User(0), movie, Rating(rating, "fake timestamp"))
    })

    movieRecommendationModelService.rateItems(rated)

    val recommendedForNewUser: List[(Movie, Rating)] = movieRecommendationModelService.getTopNForUser(User(0), 100)
    recommendedForNewUser.foreach(println(_))
  }
}