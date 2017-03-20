package gr.ml.analytics.service

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.Constants
import gr.ml.analytics.model.SparkUtil
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object PredictionService extends Constants

class PredictionService {
  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  lazy val sparkSession = SparkUtil.sparkSession()
  import sparkSession.implicits._

  def updateModel(): Unit = {
    val ratingsDF = loadRatings(PredictionService.bothRatingsPath)
    ratingsDF.show()

    val als = new ALS()
      .setMaxIter(5) // TODO extract into settable fields
      .setRegParam(0.01) // TODO extract into settable fields
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    writeModel(model)
  }

  def updatePredictionsForUser(userId: Int): java.util.List[Int] = {
    val predictedMovieIds: java.util.List[Int] = calculatePredictedIdsForUser(userId, readModel())
    persistPredictions(userId, predictedMovieIds)
    predictedMovieIds
  }

  def persistPredictions(userId: Int, predictedMovieIds: java.util.List[Int]): Unit = {
    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(PredictionService.predictionsPath, append = true)
    writer.writeRow(List(userId, predictedMovieIds.toArray.mkString(":")))
  }

  def calculatePredictedIdsForUser(userId: Int, model: ALSModel): java.util.List[Int] = {
    val toRateDS: DataFrame = getMoviesNotRatedByUser(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS).orderBy(col("prediction").desc)
    val predictedMovieIds: java.util.List[Int] = predictions.select("movieId").map(row => row.getInt(0)).collectAsList()
    predictedMovieIds
  }

  def getMoviesNotRatedByUser(userId: Int): DataFrame = {
    val reader = CSVReader.open(PredictionService.historicalRatingsPath) // TODO add support of several files, not just historical data
    // TODO could we do it more effective that reading all the file at once? Would be still OK for larger files?
    val movieIds = reader.all().filter((p:List[String])=>p(1)!="movieId" && p(0).toInt!=userId).map((p:List[String]) => p(1).toInt).toSet
    val userMovieList: List[(Int, Int)] = movieIds.map(movieId => (userId, movieId)).toList
    userMovieList.toDF("userId", "movieId")
  }

  def readModel(): ALSModel ={
    ALSModel.load(PredictionService.modelPath)
  }

  def writeModel(model: ALSModel): Unit = {
    model.write.overwrite().save(PredictionService.modelPath)
  }

  def loadRatings(path: String): DataFrame = {
    val ratingsDF = {
      val ratingsStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .load(path) // TODO can we provide several different locations?
        .select("userId", "movieId", "rating", "timestamp")

      ratingsStringDF
        .withColumn("userId", toInt(ratingsStringDF("userId")))
        .withColumn("movieId", toInt(ratingsStringDF("movieId")))
        .withColumn("rating", toDouble(ratingsStringDF("rating")))
    }
    ratingsDF
  }

}
