package model

/**
  * Base model interface
  */
trait ModelService[U, I, R] {

  case class ItemRating(item: I, rating: R)

  def topItemsForNewUser(ratedByUser: List[ItemRating], number: Int): List[(ItemRating)]

  def topItems(number: Int): List[ItemRating]

  def itemsToBeRated(number: Int): List[I]

}
