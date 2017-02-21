package gr.ml.analytics.api

import akka.actor.Actor
import gr.ml.analytics.movies.{TopNMoviesForNewUserRequest, TopNMoviesRequest}

class RecommendationActor extends Actor {

  override def receive: Receive = {
    case TopNMoviesRequest(n) =>

    case TopNMoviesForNewUserRequest(ratedMovies, n) =>

  }
}
