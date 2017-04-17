package gr.ml.analytics

package object domain {

  type Item = Map[String, Any]

  case class Rating(userId: Int, itemId: Int, rating: Double, timestamp: Long)

  object Rating

  case class Schema(schemaId: Int, jsonSchema: String)

  object Schema

}
