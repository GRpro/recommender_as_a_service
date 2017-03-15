package gr.ml.analytics


import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.io.IO
import gr.ml.analytics.api.{MoviesAPI, RatingsAPI}
import gr.ml.analytics.service.{MoviesService, MoviesServiceImpl, RatingsService, RatingsServiceImpl}
import spray.can.Http

/**
  * Application entry point
  */
object Application extends App {

  // ActorSystem to host application in
  implicit val system = ActorSystem("recommendation-service")
  val log = Logging(system, getClass)

  // create services
  val ratingsService: RatingsService = new RatingsServiceImpl()
  var moviesService: MoviesService = new MoviesServiceImpl()

  // create apis
  val ratingsApi = system.actorOf(Props[RatingsAPI](new RatingsAPI(ratingsService)), "ratings-api")
  val moviesApi = system.actorOf(Props[MoviesAPI](new MoviesAPI(moviesService)), "movies-api")

  IO(Http) ! Http.Bind(moviesApi, interface = "0.0.0.0", port = 8080)
  IO(Http) ! Http.Bind(ratingsApi, interface = "0.0.0.0", port = 8081)
}
