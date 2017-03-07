package gr.ml.analytics

import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.io.IO
import gr.ml.analytics.api.RecommendationServiceAPI
import spray.can.Http

/**
  * Application entry point
  */
object Application extends App {

  // ActorSystem to host application in
  implicit val system = ActorSystem("recommendation-api-service")
  val log = Logging(system, getClass)

  // create and start our service actor
  val service = system.actorOf(Props[RecommendationServiceAPI], "recommendation-service")

  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ! Http.Bind(service, interface = "localhost", port = 8080)
}
