package gr.ml.analytics.service

import gr.ml.analytics.domain.Rating
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

case class Item(itemId: Int)

object Item

object JsonSerDeImplicits {
  implicit val ratingFormat: RootJsonFormat[Rating] = jsonFormat3(Rating.apply)
  implicit val itemFormat: RootJsonFormat[Item] = jsonFormat1(Item.apply)
}
