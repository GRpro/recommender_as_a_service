package gr.ml.analytics.service

class RatingsServiceImpl extends RatingsService {
  /**
    * Create new ratings for a given user
    *
    * @param userId  id of the user who rated movies // TODO update
    */
  override def create(userId: Int, movieId: Int, rating: Double): Unit = {
    println("Create rating")
    new RatingService().persistRating(userId, movieId, rating)
  }

  /**
    * Get most relevant movies for a given user
    * @param userId id of the user to get recommendations for
    * @param pageSize size of page
    * @param page number of page
    * @return ordered list of movie ids
    */
  override def getTop(userId: Int, pageSize: Int, page: Int): List[Int] = { // TODO top N or pagination?
    println("Get top ratings")
    val predictedMovieIds = new RatingService().loadPredictions(userId.toInt, pageSize)
    predictedMovieIds
  }
}
