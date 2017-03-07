package gr.ml.analytics.api

import akka.actor.Actor
import gr.ml.analytics.entities._
import gr.ml.analytics.model.MovieRecommendationServiceImpl
import spray.http.{MediaTypes, StatusCodes}
import spray.httpx.SprayJsonSupport._
import spray.routing.HttpService

/**
  * Spray REST API
  */
class RecommendationServiceAPI extends Actor with HttpService {

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
    path("ratings") {
      post {
        entity(as[List[UserMovieRating]]) { ratings =>

          service.rateItems(ratings.map(userMovieRating =>
            (userMovieRating.user, userMovieRating.movie, userMovieRating.rating)))

          complete(StatusCodes.Created)
        }
      }
    } ~
    /*
    Get top N movies for a particular user
     */
    path("ratings" / LongNumber / "top" / IntNumber) { (userId, number) =>
      get {
        val topN = service.getTopNForUser(User(userId), number)
        respondWithMediaType(MediaTypes.`application/json`) {
          complete {
            topN.map(movieRating => MovieRating(movieRating._1, movieRating._2))
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