package gr.ml.analytics.model

//import java.nio.file.Paths
//
//import com.typesafe.scalalogging.LazyLogging
//import gr.ml.analytics.movies.{Movie, MovieRecommendationService, Rating, User}
//import org.apache.spark.ml.recommendation.ALS
//import org.apache.spark.sql.functions._
//
//object MovieRecommendationServiceImpl {
//
//  val datasetsDirectory = "datasets"
//  val smallDatasetFileName = "ml-latest-small.zip"
//  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"
//
//  Util.loadResource(smallDatasetUrl,
//    Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
//  Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
//    Paths.get(datasetsDirectory).toAbsolutePath)
//
//  def apply(): MovieRecommendationServiceImpl = new MovieRecommendationServiceImpl(
//    Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString,
//    Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString)
//}
//
//
//class MovieRecommendationServiceImpl(val moviesDfPath: String,
//                                     val ratingsDfPath: String) extends MovieRecommendationService with LazyLogging {
//
//  private lazy val spark = SparkUtil.sparkSession()
//
//  import spark.implicits._
//
//  private lazy val moviesDF = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") //reading the headers
//    .option("mode", "DROPMALFORMED")
//    .load(moviesDfPath)
//    .select("movieId", "title")
//
//  private lazy val ratingsDF = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") //reading the headers
//    .option("mode", "DROPMALFORMED")
//    .load(ratingsDfPath)
//    .select("userId", "movieId", "rating")
//
//  private var baseDF = ratingsDF
//    .join(moviesDF, ratingsDF("movieId") === moviesDF("movieId"))
//    .drop(ratingsDF("movieId"))
//
//  val toInt = udf[Int, String](_.toInt)
//  val toDouble = udf[Double, String](_.toDouble)
//
//
//  override def getItems(n: Int): List[Movie] = {
//    moviesDF
//      .select("movieId", "title")
//      .map(row => {
//        val movieId = row.getString(0).toInt
//        val title = row.getString(1)
//        Movie(movieId, title)
//      })
//      .take(n).toList
//  }
//
//  override def rateItems(rated: List[(User, Movie, Rating)]): Unit = {
//    val personalRatingsDF = rated
//      .map(row => (row._1.toString, row._3.toString, row._2.id.toString, row._2.title))
//      .toDF("userId", "rating", "movieId", "title")
//
//    baseDF = baseDF.union(personalRatingsDF)
//      .withColumn("userId", toInt(baseDF("userId")))
//      .withColumn("movieId", toInt(baseDF("movieId")))
//      .withColumn("rating", toDouble(baseDF("rating")))
//      .cache()
//  }
//
//  override def getTopN(n: Int): List[(Movie, Rating)] = {
//    baseDF.createOrReplaceTempView("base")
//
//    val topMovies = spark.sql(
//      "SELECT movieId, title, sum(rating) / count(userId) " +
//        "FROM base " +
//        "GROUP BY movieId, title " +
//        "ORDER BY sum(rating) DESC")
//
//    topMovies.take(n)
//      .map(row => {
//        val movieId = row.getString(0).toInt
//        val title = row.getString(1)
//        val avgRating = row.getDouble(2)
//        (Movie(movieId, title), Rating(avgRating))
//      }).toList
//  }
//
//  override def getTopNForUser(user: User, n: Int): List[(Movie, Rating)] = {
//    // Build the recommendation gr.ml.analytics.model using ALS on the training gr.ml.analytics.data
//
//    val encodedDF = baseDF
//      .withColumn("userId", toInt(baseDF("userId")))
//      .withColumn("movieId", toInt(baseDF("movieId")))
//      .withColumn("rating", toDouble(baseDF("rating")))
//      .cache()
//
//    encodedDF.createOrReplaceTempView("encoded")
//
//    val als = new ALS()
//      .setMaxIter(5)
//      .setRegParam(0.01)
//      .setUserCol("userId")
//      .setItemCol("movieId")
//      .setRatingCol("rating")
//    val model = als.fit(encodedDF)
//
//    val userRatingsDF = spark.sql("SELECT movieId FROM encoded DISTINCT")
//      .withColumn("userId", lit(user.id))
//
//    val predictions = model.transform(userRatingsDF)
//
//    val filteredPredictions = predictions.filter(not(isnan($"prediction"))).cache()
//
//    filteredPredictions.createTempView("predictions")
//
//    predictions.show()
//
//    val topRecommended = spark.sql(
//      s"SELECT movieId, title, prediction " +
//        s"FROM predictions " +
//        s"WHERE userId = ${user.id} " +
//        s"AND movieId NOT IN (SELECT movieId FROM predictions WHERE userId = ${user.id}) " +
//        s"ORDER BY prediction DESC")
//
//    topRecommended.show()
//
//    topRecommended.take(n).map(row => {
//      val movieId: Int = row.getInt(0)
//      val title: String = row.getString(1)
//      val rating: Double = row.getFloat(2).toDouble
//
//      (Movie(movieId, title), Rating(rating))
//    }).toList
//
//  }
//
//  override def addUser(user: User): User = {
//    val newUserId = ratingsDF
//      .withColumn("userId", toInt(baseDF("userId")))
//      .groupBy("userId")
//      .max("userId").head().getInt(0) + 1
//    User(newUserId)
//  }
//
//  override def addItem(item: Movie): Movie = ???
//
//
//  def topItemsForNewUser(ratedByUser: List[(Movie, Rating)], number: Int): List[(Movie, Rating)] = {
//
//    val newUserId = ratingsDF
//      .withColumn("userId", toInt(baseDF("userId")))
//      .groupBy("userId")
//      .max("userId").head().getInt(0) + 1
//
//    val personalRatingsDF = ratedByUser
//      .map(row => (newUserId.toString, row._2.toString, row._1.id.toString, row._1.title))
//      .toDF("userId", "rating", "movieId", "title")
//
//    personalRatingsDF.show()
//
//    val encodedDF = baseDF.union(personalRatingsDF)
//      .withColumn("userId", toInt(baseDF("userId")))
//      .withColumn("movieId", toInt(baseDF("movieId")))
//      .withColumn("rating", toDouble(baseDF("rating")))
//      .cache()
//
//    val Array(training, test) = encodedDF.randomSplit(Array(0.6, 0.4))
//
//    // Build the recommendation gr.ml.analytics.model using ALS on the training gr.ml.analytics.data
//    val als = new ALS()
//      .setMaxIter(5)
//      .setRegParam(0.01)
//      .setUserCol("userId")
//      .setItemCol("movieId")
//      .setRatingCol("rating")
//    val model = als.fit(training)
//
//    val predictions = model.transform(test)
//
//    val filteredPredictions = predictions.filter(not(isnan($"prediction"))).cache()
//
//    filteredPredictions.createTempView("predictions")
//    personalRatingsDF.createTempView("personal_ratings")
//
//    predictions.show()
//
//    val topRecommended = spark.sql(
//      s"SELECT movieId, title, prediction " +
//        s"FROM predictions " +
//        s"WHERE userId = $newUserId " +
//        s"AND movieId NOT IN (SELECT movieId FROM personal_ratings) " +
//        s"ORDER BY prediction DESC")
//
//    topRecommended.show()
//
//    topRecommended.take(number).map(row => {
//      val movieId: Int = row.getInt(0)
//      val title: String = row.getString(1)
//      val rating: Double = row.getFloat(2).toDouble
//
//      (Movie(movieId, title), Rating(rating))
//    }).toList
//  }
//}
