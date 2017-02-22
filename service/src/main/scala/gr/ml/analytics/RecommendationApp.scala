package gr.ml.analytics

import akka.actor._
import gr.ml.analytics.api.RecommendationActor

object RecommendationApp extends App  {
  val system = ActorSystem("RecommendationSystem")
  val remoteActor = system.actorOf(Props[RecommendationActor], name = "RecommendationActor")
  remoteActor ! Start
}

case object Start




