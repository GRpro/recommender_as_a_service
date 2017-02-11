package gr.ml.analytics

import model.{Movie, MovieRecommendationModelService}

import scala.io.StdIn

object App {

  def main(args: Array[String]): Unit = {

    val movieRecommendationModelService = MovieRecommendationModelService()

    import movieRecommendationModelService.ItemRating

    /* Top recommendations */
    val topMovies = movieRecommendationModelService.topItems(100)
    topMovies.foreach(println(_))

    val toBeRated: List[Movie] = movieRecommendationModelService.itemsToBeRated(10)
    println("Please rate the following movies [0-5]")
    val rated = toBeRated.map(movie => {
      println(s"${movie.id}, ${movie.title} [0-5]")
      val rating = StdIn.readDouble()
      ItemRating(movie, rating)
    })

    /* Top recommendation per given user */
    val recommendedForNewUser: List[ItemRating] = movieRecommendationModelService.topItemsForNewUser(rated, 100)
    recommendedForNewUser.foreach(println(_))
  }
}