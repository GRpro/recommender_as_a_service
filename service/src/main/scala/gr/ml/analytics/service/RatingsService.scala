package gr.ml.analytics.service

import gr.ml.analytics.entities.MovieRating

trait RatingsService {

  /**
    * Create new ratings for a given user
    * @param userId id of the user who rated movies
    // todo update
    */
  def create(userId: Int, movieId: Int, rating: Double)

  /**
    * Get most relevant movies for a given user
    * @param userId id of the user to get recommendations for
    * @param pageSize size of page
    * @param page number of page
    * @return ordered list of movie ids
    */
  def getTop(userId: Int, pageSize: Int, page: Int): List[Int] // TODO top N or pagination??
}
