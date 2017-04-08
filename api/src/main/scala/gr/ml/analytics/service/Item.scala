package gr.ml.analytics.service

import spray.json.{DefaultJsonProtocol, RootJsonFormat}

case class Item(itemId: Int)

object Item extends DefaultJsonProtocol {
  implicit val itemFormat: RootJsonFormat[Item] = jsonFormat1(Item.apply)
}

case class Rating(userId: Int, itemId: Int, rating: Double)

object Rating extends DefaultJsonProtocol {
  implicit val ratingFormat: RootJsonFormat[Rating] = jsonFormat3(Rating.apply)
}