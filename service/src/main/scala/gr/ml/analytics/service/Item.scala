package gr.ml.analytics.service

import spray.json.DefaultJsonProtocol

case class Item(itemId: Int)

object Item extends DefaultJsonProtocol {
  implicit val itemFormat = jsonFormat1(Item.apply)
}

case class Rating(userId: Int, itemId: Int, rating: Double)

object Rating extends DefaultJsonProtocol {
  implicit val ratingFormat = jsonFormat3(Rating.apply)
}