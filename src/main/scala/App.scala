import java.nio.file.Paths

import data.Dataset
import SparkUtil._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

import org.apache.spark.sql.functions._

import scala.io.StdIn

object App {

  val NUM_TRAINING = 10

  def personalRatings(sparkSession: SparkSession, moviesDF: DataFrame, userId: Int): DataFrame = {
    println("Please rate the following movies [0-5]")

    val rating = moviesDF
      .select("movieId", "title")
      .take(NUM_TRAINING)
      .map(row => {
        val movieId = row.getString(0).toInt
        val title = row.getString(1)

        println(s"$movieId, $title [0-5]")
        val rating = StdIn.readDouble()

        (userId.toString, rating.toString, movieId.toString, title)
      })

    val personalRatingsDF = sparkSession.createDataFrame(rating).toDF("userId", "rating", "movieId", "title")
    personalRatingsDF
  }


  def main(args: Array[String]): Unit = {


    val datasetsDirectory = "datasets"

    val smallDatasetFileName = "ml-latest-small.zip"
    val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

//    val completeDatasetFileName = "ml-latest.zip"
//    val completeDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$completeDatasetFileName"
    //    val completeDatasetPath = Dataset.loadResource(completeDatasetUrl, Paths.get(datasetsDirectory, completeDatasetFileName))
    //    Dataset.unzip(completeDatasetPath, Paths.get(datasetsDirectory))

    val smallDatasetPath = Dataset.loadResource(smallDatasetUrl, Paths.get(datasetsDirectory, smallDatasetFileName))

    Dataset.unzip(smallDatasetPath, Paths.get(datasetsDirectory))


    val spark = sparkSession()

    import spark.implicits._

    val moviesDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load(Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString)
      .select("movieId", "title")

    val ratingsDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") //reading the headers
      .option("mode", "DROPMALFORMED")
      .load(Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString)
      .select("userId", "movieId", "rating")


    val toInt    = udf[Int, String]( _.toInt)
    val toDouble = udf[Double, String]( _.toDouble)

    val joinedDF = ratingsDF
      .join(moviesDF, ratingsDF("movieId") === moviesDF("movieId"))
      .drop(ratingsDF("movieId"))

    /* Obtain ratings for special user */
    val newUserId = ratingsDF
      .withColumn("userId", toInt(joinedDF("userId")))
      .groupBy("userId")
      .max("userId").head().getInt(0) + 1

    val personalRatingsDF = personalRatings(sparkSession(), moviesDF, newUserId)
    personalRatingsDF.show()

    val encodedDF = joinedDF.union(personalRatingsDF)
      .withColumn("userId", toInt(joinedDF("userId")))
      .withColumn("movieId", toInt(joinedDF("movieId")))
      .withColumn("rating", toDouble(joinedDF("rating")))
      .cache()

    val Array(training, test) = encodedDF.randomSplit(Array(0.6, 0.4))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    val predictions = model.transform(test)

    val filteredPredictions = predictions.filter(not(isnan($"prediction"))).cache()

    filteredPredictions.createTempView("predictions")
    personalRatingsDF.createTempView("personal_ratings")

    predictions.show()

    val topRecommended = spark.sql(
      s"SELECT title, prediction " +
        s"FROM predictions " +
        s"WHERE userId = $newUserId " +
        s"AND movieId NOT IN (SELECT movieId FROM personal_ratings) " +
        s"ORDER BY prediction DESC")

    topRecommended.show()

    topRecommended.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("recommendations.csv")

//    val evaluator = new RegressionEvaluator()
//      .setMetricName("rmse")
//      .setLabelCol("rating")
//      .setPredictionCol("prediction")
//    val rmse = evaluator.evaluate(filteredPredictions)
//
//    predictions.show()
//    println(s"Root-mean-square error = $rmse")

  }
}