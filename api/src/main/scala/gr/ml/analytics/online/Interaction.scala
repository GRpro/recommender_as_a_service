package gr.ml.analytics.online


case class Interaction(userId: String, itemId: String, weight: Double, timestamp: Long)