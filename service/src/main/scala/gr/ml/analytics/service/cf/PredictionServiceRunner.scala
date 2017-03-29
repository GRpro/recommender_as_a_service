package gr.ml.analytics.service.cf

import java.nio.file.Paths

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.Constants
import gr.ml.analytics.util.{SparkUtil, Util}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

object PredictionService extends Constants {

}

class PredictionService {
  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  lazy val sparkSession = SparkUtil.sparkSession()
  import sparkSession.implicits._

  def updateModel(): Unit = {
    val ratingsDF = loadRatings(PredictionService.bothRatingsPath)

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
    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS)
      .filter(not(isnan($"prediction")))
      .orderBy(col("prediction").desc)
    val predictedMovieIds: java.util.List[Int] = predictions.select("movieId").map(row => row.getInt(0)).collectAsList()
    predictedMovieIds
  }

  def getMovieIDsNotRatedByUser(userId: Int): List[Int] = {
    val reader = CSVReader.open(PredictionService.historicalRatingsPath) // TODO add support of several files, not just historical data
    val allRatings = reader.all()
    val allMovieIDs = getAllMovieIDs()
    val movieIdsRatedByUser = allRatings.filter((p:List[String])=>p(1)!="movieId" && p(0).toInt==userId)
      .map((p:List[String]) => p(1).toInt).toSet
    val movieIDsNotRateByUser = allMovieIDs.filter(m => !movieIdsRatedByUser.contains(m))
    movieIDsNotRateByUser
  }

  def getUserMoviePairsToRate(userId: Int): DataFrame = {
    val movieIDsNotRateByUser = getMovieIDsNotRatedByUser(userId)
    val userMovieList: List[(Int, Int)] = movieIDsNotRateByUser.map(movieId => (userId, movieId))
    userMovieList.toDF("userId", "movieId")
  }

  def getAllMovieIDs(): List[Int] ={
    val reader = CSVReader.open(PredictionService.moviesPath)
    val allMovieIds = reader.all().filter(r=>r(0)!="movieId").map(r=>r(0).toInt)
    reader.close()
    allMovieIds
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



object PredictionServiceRunner extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  Util.windowsWorkAround()

  Util.loadResource(smallDatasetUrl,
    Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
  Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
    Paths.get(datasetsDirectory).toAbsolutePath)

  def tryAndLog(method: => Unit, message: String): Unit ={
    progressLogger.info(message)
    try {
      method
    } catch {
      case _: Exception => progressLogger.error("Error during " + message)
    }
  }

  /*

  Periodically run batch job which updates model

   */
  while(true) {
    Thread.sleep(1000)
    tryAndLog(predictionService.updateModel(), "Updating model")
    tryAndLog(predictionService.updatePredictionsForUser(0), "Updating predictions for User " + 0) // TODO add method getUserIdsForPrediction (return all unique user ids from current-ratings.csv)
  }

}
