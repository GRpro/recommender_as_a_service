package gr.ml.analytics.online


case class Interaction(userId: String, itemId: String, action: String, timestamp: Long)