package gr.ml.analytics.service

import scala.concurrent.Future

/**
  * Current implementation works only with IDs.
  * In future it will be extended to support
  * type-specific recommendation defined by generic.
  */
trait RecommenderService {

  /**
    * Get most relevant items for a given user
    * @param userId id of the user to get recommendations for
    * @param n number of recommendation ids to be returned
    * @return ordered list of item ids
    */
  def getTop(userId: Int, n: Int): Future[List[Int]]
}
