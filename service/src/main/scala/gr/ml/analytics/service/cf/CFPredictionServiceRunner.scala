package gr.ml.analytics.service.cf

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.Constants
import gr.ml.analytics.util.{SparkUtil, Util}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object CFPredictionService extends Constants {
  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  val sparkSession = SparkUtil.sparkSession()
  import sparkSession.implicits._

  def getUserIdsFromLastNRatings(lastN: Int): Set[Int] = {
    val reader = CSVReader.open(ratingsPath)
    val allUserIds = reader.all().filter(r => r(0) != "userId").map(r => r(0).toInt)
    val recentIds = allUserIds.splitAt(allUserIds.size - lastN)._2.toSet
    reader.close()
    recentIds
  }

  def persistPopularItemIDS(): Unit ={
    val ratingsReader = CSVReader.open(ratingsPath)
    val allRatings = ratingsReader.all()
    ratingsReader.close()
    val mostPopular = allRatings.filter(l=>l(1)!="movieId")
      .groupBy(l=>l(1))
      .map(t=>(t._1, t._2, t._2.size))
      .map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble + l2(2).toDouble).toString, l1(3))), t._3))
      .map(t=>(t._1,t._2(2).toDouble / t._3.toDouble, t._3))
      .toList.sortWith((tl,tr) => tl._3 > tr._3) // sorting by number of ratings
      .take(allRatings.size/10) // take first 1/10 of items sorted by number of ratings

    val maxRating: Double = mostPopular.sortWith((tl,tr)=>tl._2 > tr._2).head._2
    val maxNumberOfRatings: Int = mostPopular.sortWith((tl,tr)=>tl._3 > tr._3).head._3

    val sorted = mostPopular.sortWith(sortByRatingAndPopularity(maxRating,maxNumberOfRatings))
      .map(t=>List(t._1, t._2, t._3))

    val popularItemsHeaderWriter = CSVWriter.open(popularItemsPath, append = false)
    popularItemsHeaderWriter.writeRow(List("itemId", "rating", "nRatings"))
    popularItemsHeaderWriter.close()

    val popularItemsWriter = CSVWriter.open(popularItemsPath, append = true)
    popularItemsWriter.writeAll(sorted)
    popularItemsWriter.close()
  }

  def sortByRatingAndPopularity(maxRating:Double, maxRatingsNumber:Int) ={
    // Empirical coefficient to make popular high rated movies go first
    // (suppressing unpopular but high-rated movies by small number of individuals)
    // Math.PI is just for more "scientific" look ;-)
    val coef = Math.PI * Math.sqrt(maxRatingsNumber.toDouble)/Math.sqrt(maxRating)
    (tl:(String,Double,Int), tr:(String,Double,Int)) =>
      Math.sqrt(tl._3) + coef * Math.sqrt(tl._2) > Math.sqrt(tr._3) + coef * Math.sqrt(tr._2)
  }

  def updateModel(): Unit = {
    val ratingsDF = loadRatings()

    val als = new ALS()
      .setMaxIter(5) // TODO extract into settable fields
      .setRegParam(0.01) // TODO extract into settable fields
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    writeModel(model)
  }


  def readModel(): ALSModel ={
    ALSModel.load(collaborativeModelPath)
  }

  def writeModel(model: ALSModel): Unit = {
    model.write.overwrite().save(collaborativeModelPath)
  }

  def loadRatings(): DataFrame = {
    val ratingsDF = {
      val ratingsStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .load(CFPredictionService.ratingsPath)
        .select("userId", "movieId", "rating", "timestamp")

      ratingsStringDF
        .withColumn("userId", toInt(ratingsStringDF("userId")))
        .withColumn("movieId", toInt(ratingsStringDF("movieId")))
        .withColumn("rating", toDouble(ratingsStringDF("rating")))
    }
    ratingsDF
  }
  def updatePredictionsForUser(userId: Int): DataFrame = {
    val predictions: DataFrame = calculatePredictionsForUser(userId, readModel())
    new File(collaborativePredictionsDirectoryPath).mkdirs()
    persistPredictionsForUser(userId, predictions, String.format(collaborativePredictionsForUserPath, userId.toString))
    predictions
  }

  def persistPredictedIdsForUser(userId: Int, predictedItemIds: List[Int]): Unit = {
    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(predictionsPath, append = true)
    writer.writeRow(List(userId, predictedItemIds.toArray.mkString(":")))
  }

  def persistPredictionsForUser(userId: Int, predictions: DataFrame, path: String): Unit = {
    val predictionsHeaderWriter = CSVWriter.open(path, append = false)
    predictionsHeaderWriter.writeRow(List("userId","itemId","prediction"))
    predictionsHeaderWriter.close()

    val predictionsWriter = CSVWriter.open(String.format(path), append = true)
    val predictionsList = predictions.rdd.map(r=>List(userId,r(r.fieldIndex("itemId")).toString.toDouble.toInt,r(r.fieldIndex("prediction")))).collect()
    predictionsWriter.writeAll(predictionsList)
    predictionsWriter.close()
  }

  def calculatePredictionsForUser(userId: Int, model: ALSModel): DataFrame = {
    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS)
      .filter(not(isnan($"prediction")))
      .orderBy(col("prediction").desc)
    predictions
  }

  def calculatePredictedIdsForUser(userId: Int, model: ALSModel): List[Int] = {
    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS)
      .filter(not(isnan($"prediction")))
      .orderBy(col("prediction").desc)
    val predictedItemIds: List[Int] = predictions.select("itemId").map(row => row.getInt(0)).collect().toList
    predictedItemIds
  }

  def getItemIDsNotRatedByUser(userId: Int): List[Int] = {
    val ratingsReader = CSVReader.open(ratingsPath)
    val allRatings = ratingsReader.all()
    ratingsReader.close()

    val allMovieIDs = getAllItemIDs()
    val movieIdsRatedByUser = allRatings.filter((p:List[String])=>p(1)!="movieId" && p(0).toInt==userId)
      .map((p:List[String]) => p(1).toInt).toSet
    val movieIDsNotRateByUser = allMovieIDs.filter(m => !movieIdsRatedByUser.contains(m))
    movieIDsNotRateByUser
  }

  def getUserMoviePairsToRate(userId: Int): DataFrame = {
    val itemIDsNotRateByUser = getItemIDsNotRatedByUser(userId)
    val userMovieList: List[(Int, Int)] = itemIDsNotRateByUser.map(itemId => (userId, itemId))
    userMovieList.toDF("userId", "itemId")
  }

  def getAllItemIDs(): List[Int] ={
    val reader = CSVReader.open(moviesPath)
    val allItemIds = reader.all().filter(r=>r(0)!="itemId").map(r=>r(0).toInt)
    reader.close()
    allItemIds
  }

  def getAllUserIds(): Set[Int] ={
    val reader = CSVReader.open(ratingsPath)
    val allUserIds = reader.all().filter(r=>r(0)!="userId").map(r=>r(0).toInt).toSet
    reader.close()
    allUserIds
  }
}

class CFPredictionService {
}

object CFPredictionServiceRunner extends App with Constants {

  Util.windowsWorkAround()

  /*
    Periodically run batch job which updates model
  */
  while(true) {
    Thread.sleep(1000)
    Util.tryAndLog(CFPredictionService.updateModel(), "Collaborative:: Updating model")
    val userIds: Set[Int] = CFPredictionService.getUserIdsFromLastNRatings(1000)
    for(userId <- userIds){
      Util.tryAndLog(CFPredictionService.updatePredictionsForUser(userId), "Collaborative:: Updating predictions for User " + userId)
    }
  }
}
