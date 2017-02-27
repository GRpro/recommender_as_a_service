package controllers

import javax.inject._

import akka.actor.ActorSystem
import gr.ml.analytics.movies._
import play.api._
import play.api.mvc._
import play.api.libs.concurrent.Execution.Implicits.defaultContext

import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import play.api.libs.json._

import scala.concurrent.Future
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._ // Combinator syntax

/**
 * This controller creates an `Action` to handle HTTP requests to the
 * application's home page.
 */
@Singleton
class MainController @Inject()(system: ActorSystem) extends Controller {

  val masterActorSystem = ActorSystem.create("RecommendationSystem", ConfigFactory.load().getConfig("RecommendationSystem"))

  val recommendationActor = masterActorSystem.actorSelection("akka.tcp://RecommendationSystem@127.0.0.1:5150/user/recommendation-actor")

  implicit val timeout: Timeout = 120.seconds

//  implicit def reads(json: JsValue): List[(User, Movie, Rating)] = {
//    val result = json.as[List[JsValue]].map(umr => {
//      val userId = (umr \ "userId").as[Long]
//      val movieId = (umr \ "movieId").as[Long]
//      val rating = (umr \ "rating").as[Double]
//      val timestamp = (umr \ "timestamp").as[String]
//      (User(userId), Movie(movieId, null, null, null, null), Rating(rating, timestamp))
//    })
//    result
//  }
//
//  implicit val userWrites = new Writes[User] {
//    override def writes(o: User): JsValue = Json.obj(
//      "userId" -> o.userId
//    )
//  }
//
//  implicit val movieWrites = new Writes[Movie] {
//    override def writes(o: Movie): JsValue = Json.obj(
//      "movieId" -> o.movieId,
//      "movieTitle" -> o.title,
//      "movieGenres" -> o.genres,
//      "movieImdbId" -> o.imdbId,
//      "movieTmdbId" -> o.tmdbId
//    )
//  }
//
//  implicit val ratingWrites = new Writes[Rating] {
//    override def writes(o: Rating): JsValue = Json.obj(
//      "rating" -> o.rating,
//      "timestamp" -> o.timestamp
//    )
//  }


//  implicit val userReads = new Reads[User] {
//    override def reads(json: JsValue): JsResult[User] = {
//      val userId = (json \ "userId").as[Long]
//      User(userId)
//      JsResult()
//    }
//  }



//  implicit val movieReads = new Reads[Movie] {
//    override def reads(json: JsValue): JsResult[Movie] = (JsPath \ "movieId").read[Long](Movie.apply(_, null, null, null, null))
//  }



//  implicit val userMovieRatingWrites = new Writes[List[(User, Movie, Rating)]] {
//    override def writes(o: List[(User, Movie, Rating)]): JsValue = Json.obj(
//      o.map(userMovieRating => {
//        "userId" -> userMovieRating._1.id
//        "movieId" -> userMovieRating._2.id
//        "movieTitle" -> userMovieRating._2.title
//        "movieGanres" -> userMovieRating._2.genres
//        "movieImdbId" -> userMovieRating._2.imdbId
//        "movieTmdbId" -> userMovieRating._2.tmdbId
//        "rating" -> userMovieRating._3.rating
//        "timestamp" -> userMovieRating._3.timestamp
//      })
//    )
//    }
//  }


  import play.api.libs.concurrent.Execution.Implicits.defaultContext



  implicit val userReads = Json.reads[User]
  implicit val userWrites = Json.writes[User]

  implicit val movieReads = Json.reads[Movie]
  implicit val movieWrites = Json.writes[Movie]

  implicit val ratingReads = Json.reads[Rating]
  implicit val ratingWrites = Json.writes[Rating]

  implicit val listUserMovieRatingWrites = Json.writes[UserMovieRating]

  implicit val listUserMovieRatingReads = Json.reads[UserMovieRating]

  def recommendationForUser(userId: Int) = Action.async {
    val user = User(userId)
    (recommendationActor ? TopNMoviesForUserRequest(user, 10)).mapTo[TopNMoviesForUser].map {
      response => Ok(Json.toJson(response.topN.map(umr => UserMovieRating(umr._1, umr._2, umr._3))))
    }
  }

  def getItems(n: Int) = Action.async {
    (recommendationActor ? ItemsToBeRatedRequest(n)).mapTo[ItemsToBeRated].map {
      response => Ok(Json.toJson(response.items))
    }
  }

  def rate = Action { request =>

    val json = request.body.asJson.get

    val ratings: List[UserMovieRating] = json.as[List[UserMovieRating]]

    recommendationActor ! RateItems(ratings.map(umr => (umr.user, umr.movie, umr.rating)))
    Ok
  }


  /**
   * Create an Action to render an HTML page.
   *
   * The configuration in the `routes` file means that this method
   * will be called when the application receives a `GET` request with
   * a path of `/`.
   */
  def index = Action { implicit request =>
    Ok(views.html.index())
  }
}
