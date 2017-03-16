package gr.ml.analytics.service

import gr.ml.analytics.entities.{MovieRating, PredictedMovies}

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
    * @return list of list of [[MovieRating]] objects // TODO update
    */
  def getTop(userId: Int, pageSize: Int, page: Int): PredictedMovies // TODO top N or pagination??
}
