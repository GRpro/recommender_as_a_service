package gr.ml.analytics

/**
  * Generic recommendation service interface.
  * Implementors use it to define services for different types of user-to-item model.
  *
  * @tparam U user type
  * @tparam I item type
  * @tparam R rating type
  */
trait RecommendationService[U, I, R] {

  def getItems(n: Int): List[I]

  def rateItems(rated: List[(U, I, R)])

  def getTopNForUser(user: U, n: Int): List[(I, R)]

}
