package gr.ml.analytics

//import gr.ml.analytics.model.MovieRecommendationServiceImpl
//import gr.ml.analytics.movies.{Movie, Rating, User}
//
//import scala.io.StdIn
//
//object TestApplication {
//
//  def main(args: Array[String]): Unit = {
//
//    val movieRecommendationModelService = MovieRecommendationServiceImpl()
//
//
//    /* Top recommendations */
//    val topMovies = movieRecommendationModelService.getTopN(100)
//    topMovies.foreach(println(_))
//
//    val toBeRated: List[movies.Movie] = movieRecommendationModelService.getItems(10)
//    println("Please rate the following movies [0-5]")
//    val rated = toBeRated.map(movie => {
//      println(s"${movie.id}, ${movie.title} [0-5]")
//      val rating = StdIn.readDouble()
//      (movie, rating)
//    })
//
//    /* Top recommendation per given user */
//    val newUser = movieRecommendationModelService.addUser(User(0))
//    val recommendedForNewUser: List[(Movie, Rating)] = movieRecommendationModelService.getTopNForUser(newUser, 100)
//    recommendedForNewUser.foreach(println(_))
//  }
//}