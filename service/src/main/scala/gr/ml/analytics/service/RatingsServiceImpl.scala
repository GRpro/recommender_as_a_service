package gr.ml.analytics.service
import gr.ml.analytics.entities.MovieRating

class RatingsServiceImpl extends RatingsService {
  /**
    * Create new ratings for a given user
    *
    * @param userId  id of the user who rated movies
    * @param ratings list of [[MovieRating]] objects that represents ratings to specific movies
    */
  override def create(userId: Long, ratings: List[MovieRating]): Unit = {
    println(s"Create ratings")
  }

  /**
    * Get most relevant movies for a given user
    *
    * @param userId   id of the user to get recommendations for
    * @param pageSize size of page
    * @param page     number of page
    * @return list of list of [[MovieRating]] objects
    */
  override def getTop(userId: Long, pageSize: Int, page: Int): List[MovieRating] = {
    println(s"Get top ratings")
    null
  }
}
