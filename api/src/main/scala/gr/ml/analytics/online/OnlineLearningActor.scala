package gr.ml.analytics.online

import akka.actor.Actor
import com.typesafe.scalalogging.LazyLogging

class OnlineLearningActor(val itemItemRecommender: ItemItemRecommender) extends Actor with LazyLogging {

  override def receive: Receive = {
    case (interaction: Interaction, weight: Double) =>

      itemItemRecommender.learn(interaction, weight.toInt)

      logger.info(s"Received interaction $interaction")
    case _ =>
      throw new RuntimeException("Unknown message")
  }
}
