package gr.ml.analytics.service

trait RatingService {

  /**
    * Create new ratings for a given user
    * @param userId id of the user who rated movies
    // todo update
    */
  def save(userId: Int, movieId: Int, rating: Double)

  /**
    * Get most relevant movies for a given user
    * @param userId id of the user to get recommendations for
    * @param n number of recommendation ids to be returned
    * @return ordered list of movie ids
    */
  def getTop(userId: Int, n: Int): List[Int]
}
