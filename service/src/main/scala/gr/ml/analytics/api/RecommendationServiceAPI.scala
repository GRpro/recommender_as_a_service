package gr.ml.analytics.api

import akka.actor.Actor
import gr.ml.analytics.entities._
import gr.ml.analytics.model.MovieRecommendationServiceImpl
import spray.http.HttpHeaders.RawHeader
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.routing.HttpService
import spray.http._
import spray.routing._

/**
  * Spray REST API
  */
class RecommendationServiceAPI extends Actor with HttpService with CORSSupport {

  def actorRefFactory = context

  override def receive: Receive = runRoute(ratingsRoute ~ moviesRoute)

  val service: MovieRecommendationService = MovieRecommendationServiceImpl()

  /**
    * Ratings API
    */
  private val ratingsRoute = {
    /*
    Create new User Movie Rating relationships
    */

      respondWithHeaders(
        RawHeader("Access-Control-Allow-Origin", "http://localhost:63342"),
        RawHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS"),
        RawHeader("Access-Control-Allow-Headers", "Cache-Control, Pragma, Origin, Authorization, Content-Type, X-Requested-With")) {
      path("ratings") {
        post {
          entity(as[List[UserMovieRating]]) { ratings =>

            service.rateItems(ratings.map(userMovieRating =>
              (userMovieRating.user, userMovieRating.movie, userMovieRating.rating)))

            respondWithMediaType(MediaTypes.`application/json`) {
              complete(StatusCodes.Created)
            }
          }
        }
      } ~ options {
        complete(StatusCodes.OK)
      }} ~ /*
      Get top N movies for a particular user
       */
      cors {
        path("ratings" / LongNumber / "top" / IntNumber) { (userId, number) =>
          get {
            val topN = service.getTopNForUser(User(userId), number)
            respondWithMediaType(MediaTypes.`application/json`) {
              complete {
                val converted = topN.map(movieRating => MovieRating(movieRating._1, movieRating._2))
                converted
              }
            }
          }
        }
      }
  }

  /**
    * Movies API
    */
  private val moviesRoute = {
    /*
    Get N movies to be rated
    TODO make search APIs.
     */
    cors {
      path("movies" / IntNumber) { number =>
        get {
          respondWithMediaType(MediaTypes.`application/json`) {
            val movies = service.getItems(number)
            complete {
              movies
            }
          }
        }
      }
    }

  }

}

trait CORSDirectives { this: HttpService =>
  private def respondWithCORSHeaders(origin: String) =
    respondWithHeaders(
      HttpHeaders.`Access-Control-Allow-Origin`(SomeOrigins(List(origin))),
      HttpHeaders.`Access-Control-Allow-Credentials`(true),
      HttpHeaders.`Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.PUT, HttpMethods.DELETE, HttpMethods.OPTIONS),
      HttpHeaders.`Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Authorization")
    )
  private def respondWithCORSHeadersAllOrigins =
    respondWithHeaders(
      HttpHeaders.`Access-Control-Allow-Origin`(AllOrigins),
      HttpHeaders.`Access-Control-Allow-Credentials`(true),
      HttpHeaders.`Access-Control-Allow-Methods`(HttpMethods.GET, HttpMethods.POST, HttpMethods.PUT, HttpMethods.DELETE, HttpMethods.OPTIONS),
      HttpHeaders.`Access-Control-Allow-Headers`("Origin, X-Requested-With, Content-Type, Accept, Authorization")
    )

  def corsFilter(origins: List[String])(route: Route) =
    if (origins.contains("*"))
//      respondWithCORSHeaders("http://localhost")
      respondWithCORSHeadersAllOrigins(route)
    else
      optionalHeaderValueByName("Origin") {
        case None =>
          route
        case Some(clientOrigin) => {
          if (origins.contains(clientOrigin))
            respondWithCORSHeaders(clientOrigin)(route)
          else {
            // Maybe, a Rejection will fit better
            complete(StatusCodes.Forbidden, "Invalid origin")
          }
        }
      }
}