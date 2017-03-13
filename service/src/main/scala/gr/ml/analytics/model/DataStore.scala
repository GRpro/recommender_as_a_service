package gr.ml.analytics.model

import java.nio.file.Paths

import gr.ml.analytics.entities.{Movie, Rating, User}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object DataStore {

  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toLong: UserDefinedFunction = udf[Long, String](_.toLong)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  def apply(sparkSession: SparkSession): DataStore = {

    val datasetsDirectory = "datasets"
    val smallDatasetFileName = "ml-latest-small.zip"
    val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

    Util.loadResource(smallDatasetUrl,
      Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
    Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
      Paths.get(datasetsDirectory).toAbsolutePath)

    val moviesDfPath = Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString
    val ratingsDFPath = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
    val linksDfPath = Paths.get(datasetsDirectory, "ml-latest-small", "links.csv").toAbsolutePath.toString
    val tagsDFPath = Paths.get(datasetsDirectory, "ml-latest-small", "tags.csv").toAbsolutePath.toString

    /* Current implementation support storing data as Spark DataFrames */

    val linksDF = {
      val linksStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load(linksDfPath)
        .select("movieId", "imdbId", "tmdbId")

      linksStringDF
        .withColumn("movieId", toInt(linksStringDF("movieId")))
    }

    val moviesDF = {
      val moviesStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load(moviesDfPath)
        .select("movieId", "title", "genres")

      moviesStringDF
        .withColumn("movieId", toInt(moviesStringDF("movieId")))
    }

    val ratingsDF = {
      val ratingsStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .load(ratingsDFPath)
        .select("userId", "movieId", "rating", "timestamp")

      ratingsStringDF
        .withColumn("userId", toInt(ratingsStringDF("userId")))
        .withColumn("movieId", toInt(ratingsStringDF("movieId")))
        .withColumn("rating", toDouble(ratingsStringDF("rating")))
    }

    new DataStore(sparkSession, moviesDF, ratingsDF, linksDF, null)
  }

}


/**
  * Manages user-to-item data. Trains prediction model, while does not store it.
  */
case class DataStore(sparkSession: SparkSession,
                     moviesDF: DataFrame,
                     ratingsDF: DataFrame,
                     linksDF: DataFrame,
                     tagsDF: DataFrame) {

  import sparkSession.implicits._

  /**
    * Retrieve movies to rate
    */
  def movies(n: Int): List[Movie] = {
    val resultDF = moviesDF
      .join(linksDF, "movieId")
      .select("movieId", "title", "genres", "imdbId", "tmdbId")
      .map(row => {
        val movieId = row.getInt(0)
        val title = row.getString(1)
        val genres = row.getString(2)
        val imdbId = row.getString(3)
        val tmdbId = row.getString(4)
        Movie(movieId, title, genres, imdbId, tmdbId)
      })

    if (n > 0) resultDF.take(n).toList
    else resultDF.collect().toList
  }

  /**
    * Add new ratings
    */
  def rate(rated: List[(User, Movie, Rating)]): DataStore = {

//    val newLinksDF = rated
//      .map(row => (row._2.id, row._2.imdbId, row._2.tmdbId))
//      .toDF("movieId", "imdbId", "tmdbId")
//
//    val newMoviesDF = rated
//      .map(row => (row._2.id, row._2.title, row._2.genres))
//      .toDF("movieId", "title", "genres")

    val newRatingsDF = rated
      .map(row => (row._1.userId, row._2.movieId, row._3.rating, row._3.timestamp))
      .toDF("userId", "movieId", "rating", "timestamp")

    copy(ratingsDF = ratingsDF.union(newRatingsDF))
  }

  def composedDF = ratingsDF
    .join(moviesDF, "movieId")
    .join(linksDF, "movieId")
    .select("userId", "movieId", "rating", "title", "genres", "imdbId", "tmdbId")
    .cache()

  def trainModel: PredictionModelCreator = {

    //    val relationsDF = baseDF
    //      .withColumn("userId", toInt(baseDF("userId")))
    //      .withColumn("movieId", toInt(baseDF("movieId")))
    //      .withColumn("rating", toDouble(baseDF("rating")))
    //      .cache()

    //TODO cross validation
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    new PredictionModelCreator(sparkSession, als.fit(composedDF))
  }
}


/**
  * Manages trained model that is capable of making predictions for existing users
  */
class PredictionModelCreator(val sparkSession: SparkSession,
                             val model: ALSModel) {

  import sparkSession.implicits._

  def buildPredictionModel(data: DataStore): PredictionModel = {

    val predictions = model.transform(data.composedDF)

    val filteredPredictions = predictions.filter(not(isnan($"prediction")))

    new PredictionModel(sparkSession, filteredPredictions)
  }

}


class PredictionModel(val sparkSession: SparkSession,
                      val predictedDF: DataFrame) {


  /**
    * Predicts top N relevant movies for a given user
    */
  def getTopNForUser(user: User, n: Int): List[(Movie, Rating)] = {

    val viewName = "predictions"

    predictedDF.createOrReplaceTempView(viewName)

    val topRecommended = sparkSession.sql(
      s"SELECT movieId, title, genres, imdbId, tmdbId, prediction " +
        s"FROM $viewName " +
        s"WHERE userId = ${user.userId} " +
        s"ORDER BY prediction DESC")

    topRecommended.show()

    topRecommended.take(n).map(row => {
      val movieId: Long = row.getLong(0)
      val title: String = row.getString(1)
      val genres: String = row.getString(2)
      val imdbld: String = row.getString(3)
      val tmdbld: String = row.getString(4)
      val rating: Double = row.getFloat(5).toDouble

      (Movie(movieId, title, genres, imdbld, tmdbld), Rating(rating, null))
    }).toList
  }
}


//  /**
//    * Returns top N most popular items
//    */
//  def getTopN(n: Int): List[(Movie, Rating)] = {
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

