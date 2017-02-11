package gr.ml.analytics.model

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.functions._

case class Movie(id: Long, title: String)

object MovieRecommendationModelService {

  val datasetsDirectory = "datasets"
  val smallDatasetFileName = "ml-latest-small.zip"
  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

  Dataset.loadResource(smallDatasetUrl,
    Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
  Dataset.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
    Paths.get(datasetsDirectory).toAbsolutePath)

  def apply(): MovieRecommendationModelService = new MovieRecommendationModelService(
    Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString,
    Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString)
}


class MovieRecommendationModelService(val moviesDfPath: String,
                                      val ratingsDfPath: String)
  extends ModelService[Long, Movie, Double] with LazyLogging {


  private lazy val spark = SparkUtil.sparkSession()

  import spark.implicits._

  private lazy val moviesDF = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(moviesDfPath)
    .select("movieId", "title")

  private lazy val ratingsDF = spark.read
    .format("com.databricks.spark.csv")
    .option("header", "true") //reading the headers
    .option("mode", "DROPMALFORMED")
    .load(ratingsDfPath)
    .select("userId", "movieId", "rating")

  private lazy val baseDF = ratingsDF
      .join(moviesDF, ratingsDF("movieId") === moviesDF("movieId"))
      .drop(ratingsDF("movieId"))

  val toInt    = udf[Int, String]( _.toInt)
  val toDouble = udf[Double, String]( _.toDouble)


  override def topItemsForNewUser(ratedByUser: List[ItemRating], number: Int): List[ItemRating] = {

    val newUserId = ratingsDF
      .withColumn("userId", toInt(baseDF("userId")))
      .groupBy("userId")
      .max("userId").head().getInt(0) + 1

    val personalRatingsDF = ratedByUser
      .map(row => (newUserId.toString, row.rating.toString, row.item.id.toString, row.item.title))
      .toDF("userId", "rating", "movieId", "title")

    personalRatingsDF.show()

    val encodedDF = baseDF.union(personalRatingsDF)
      .withColumn("userId", toInt(baseDF("userId")))
      .withColumn("movieId", toInt(baseDF("movieId")))
      .withColumn("rating", toDouble(baseDF("rating")))
      .cache()

    val Array(training, test) = encodedDF.randomSplit(Array(0.6, 0.4))

    // Build the recommendation gr.ml.analytics.model using ALS on the training gr.ml.analytics.data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    val predictions = model.transform(test)

    val filteredPredictions = predictions.filter(not(isnan($"prediction"))).cache()

    filteredPredictions.createTempView("predictions")
    personalRatingsDF.createTempView("personal_ratings")

    predictions.show()

    val topRecommended = spark.sql(
      s"SELECT movieId, title, prediction " +
        s"FROM predictions " +
        s"WHERE userId = $newUserId " +
        s"AND movieId NOT IN (SELECT movieId FROM personal_ratings) " +
        s"ORDER BY prediction DESC")

    topRecommended.show()

    topRecommended.take(number).map(row => {
      val movieId: Int = row.getInt(0)
      val title: String = row.getString(1)
      val rating: Double = row.getFloat(2).toDouble

      ItemRating(Movie(movieId, title), rating)
    }).toList


    //    topRecommended.write
    //      .format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .save("recommendations.csv")

    //    // Evaluate the gr.ml.analytics.model by computing the RMSE on the test gr.ml.analytics.data
    //    val evaluator = new RegressionEvaluator()
    //      .setMetricName("rmse")
    //      .setLabelCol("rating")
    //      .setPredictionCol("prediction")
    //    val rmse = evaluator.evaluate(filteredPredictions)
    //
    //    predictions.show()
    //    println(s"Root-mean-square error = $rmse")
  }

  override def topItems(number: Int): List[ItemRating] = {
    baseDF.createOrReplaceTempView("base")
    val topMovies = spark.sql(
      "SELECT movieId, title, sum(rating) / count(userId) " +
        "FROM base " +
        "GROUP BY movieId, title " +
        "ORDER BY sum(rating) DESC")
    topMovies.take(number)
        .map(row => {
          val movieId = row.getString(0).toInt
          val title = row.getString(1)
          val avgRating = row.getDouble(2)
          ItemRating(Movie(movieId, title), avgRating)
        }).toList
  }

  override def itemsToBeRated(number: Int): List[Movie] = {
    moviesDF
      .select("movieId", "title")
      .take(number)
      .map(row => {
        val movieId = row.getString(0).toInt
        val title = row.getString(1)
        Movie(movieId, title)
      }).toList
  }
}
