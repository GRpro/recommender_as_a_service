package gr.ml.analytics.model

//import com.typesafe.scalalogging.LazyLogging
//import gr.ml.analytics.{Movie, MovieRecommendationServiceAPI, Rating, User}
//
//object MovieRecommendationServiceAPIImpl {
//
//  def apply(dataStore: DataStore): MovieRecommendationServiceAPIImpl = new MovieRecommendationServiceAPIImpl(dataStore)
//
//  def apply(): MovieRecommendationServiceAPIImpl = {
//    val sparkSession = SparkUtil.sparkSession()
//    val dataStore = DataStore(sparkSession)
//    apply(dataStore)
//  }
//}
//
//class MovieRecommendationServiceAPIImpl(var dataStore: DataStore) extends MovieRecommendationServiceAPI with LazyLogging {
//  var model: Option[PredictionModel] = None
//  override def getItems(n: Int): List[Movie] = {
//    dataStore.movies(n)
//  }
//
//  override def rateItems(rated: List[(User, Movie, Rating)]): Unit = {
//    dataStore = dataStore.rate(rated)
//    model = Some(dataStore.trainModel.buildPredictionModel(dataStore))
//  }
//
//  override def getTopNForUser(user: User, n: Int): List[(Movie, Rating)] = {
//    model match {
//      case Some(m) =>
//        m.getTopNForUser(user, n)
//      case None =>
//        model = Some(dataStore.trainModel.buildPredictionModel(dataStore))
//        List()
//    }
//
//  }
//
//}
