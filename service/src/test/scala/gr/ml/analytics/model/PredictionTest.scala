package gr.ml.analytics.model

import gr.ml.analytics.entities.{Movie, Rating, User}
import org.scalatest._


/**
  * @author hrozhkov
  */
class PredictionTest extends FlatSpec {

  lazy val sparkSession = SparkUtil.sparkSession()

  var topMovies: List[Movie] = null
  var dataStore: DataStore = null
  var predictionModelCreator: PredictionModelCreator = null
  var predictionModel: PredictionModel = null
  val user = User(0)
  var movie: Movie = null

  "DataStore" should "be instantiated without errors" in {
    dataStore = DataStore(sparkSession)
  }

  "DataStore" should "return 100 movies without errors" in {
    topMovies = dataStore.movies(100)
  }

  "DataStore" should "accept new ratings without errors" in {
    val rating = Rating(3.2, System.currentTimeMillis().toString)

    movie = topMovies((math.random * 99).toInt)

    dataStore = dataStore.rate(List((user, movie, rating)))
  }

  "PredictionModelCreator" should "be trained without errors" in {
    predictionModelCreator = dataStore.trainModel
  }

  "PredictionModel" should "be built without errors" in {
    predictionModel = predictionModelCreator.buildPredictionModel(dataStore)
  }

  "Prediction of top items for the user" should "proceed correctly with no errors" in {
    val topMovies = predictionModel.getTopNForUser(user, 10)
    assert(movie === topMovies.head._1)
  }





}
