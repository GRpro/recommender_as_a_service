package gr.ml.analytics.domain

case class Rating(userId: Int, itemId: Int, rating: Double, timestamp: Long)

object Rating