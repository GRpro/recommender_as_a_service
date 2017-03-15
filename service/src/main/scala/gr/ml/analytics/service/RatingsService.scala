package gr.ml.analytics.service

import gr.ml.analytics.entities.MovieRating

trait RatingsService {

  /**
    * Create new ratings for a given user
    * @param userId id of the user who rated movies
    * @param ratings list of [[MovieRating]] objects that represents ratings to specific movies
    */
  def create(userId: Long, ratings: List[MovieRating])

  /**
    * Get most relevant movies for a given user
    * @param userId id of the user to get recommendations for
    * @param pageSize size of page
    * @param page number of page
    * @return list of list of [[MovieRating]] objects
    */
  def getTop(userId: Long, pageSize: Int, page: Int): List[MovieRating]
}
