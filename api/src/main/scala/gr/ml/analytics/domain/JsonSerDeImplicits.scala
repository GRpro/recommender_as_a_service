package gr.ml.analytics.domain

import spray.json.RootJsonFormat
import spray.json.DefaultJsonProtocol._

object JsonSerDeImplicits {
  implicit val ratingFormat: RootJsonFormat[Rating] = jsonFormat4(Rating.apply)
  implicit val itemFormat: RootJsonFormat[Item] = jsonFormat1(Item.apply)
}
