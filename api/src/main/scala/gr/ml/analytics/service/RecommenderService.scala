package gr.ml.analytics.service

/**
  * Current implementation works only with IDs.
  * In future it will be extended to support
  * type-specific recommendation defined by generic.
  */
trait RecommenderService {

  /**
    * Create new ratings for a given user
    * @param userId id of the user who rated items
    */
  def save(userId: Int, itemId: Int, rating: Double, timestamp: Long)

  /**
    * Get most relevant items for a given user
    * @param userId id of the user to get recommendations for
    * @param n number of recommendation ids to be returned
    * @return ordered list of item ids
    */
  def getTop(userId: Int, n: Int): List[Int]
}
