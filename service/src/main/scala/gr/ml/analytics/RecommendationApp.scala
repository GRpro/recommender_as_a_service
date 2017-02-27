package gr.ml.analytics

import akka.actor._
import com.typesafe.config.ConfigFactory
import gr.ml.analytics.api.RecommendationActor


object RecommendationApp extends App  {
  private val root = ConfigFactory.load()

  val system = ActorSystem("RecommendationSystem", root.getConfig("RecommendationSystem"))

  val remoteActor = system.actorOf(Props[RecommendationActor], name = "recommendation-actor")
  remoteActor ! Start
}

case object Start




