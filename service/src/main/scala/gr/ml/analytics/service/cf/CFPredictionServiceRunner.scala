package gr.ml.analytics.service.cf

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.Constants
import gr.ml.analytics.util.{DataUtil, SparkUtil, Util}
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class CFPredictionService(val subRootDir: String) extends Constants {
  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)
  val dataUtil = new DataUtil(subRootDir)

  def persistPopularItems(): Unit ={
    println("persistPopularItems")
    val ratingsReader = CSVReader.open(String.format(ratingsPath,subRootDir))
    val allRatings = ratingsReader.all()
    ratingsReader.close()
    val mostPopular = allRatings.filter(l=>l(1)!="itemId")
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

    val popularItemsHeaderWriter = CSVWriter.open(String.format(popularItemsPath, subRootDir), append = false)
    popularItemsHeaderWriter.writeRow(List("itemId", "rating", "nRatings"))
    popularItemsHeaderWriter.close()

    val popularItemsWriter = CSVWriter.open(String.format(popularItemsPath, subRootDir), append = true)
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
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    writeModel(model)
  }


  def readModel(): ALSModel ={
    val spark = SparkUtil.sparkSession()
    val model = ALSModel.load(String.format(collaborativeModelPath, subRootDir))
    model
  }

  def writeModel(model: ALSModel): Unit = {
    model.write.overwrite().save(String.format(collaborativeModelPath, subRootDir))
  }

  def loadRatings(): DataFrame = {
    val ratingsDF = {
      val ratingsStringDF = SparkUtil.sparkSession().read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .load(String.format(ratingsPath, subRootDir))
        .select("userId", "itemId", "rating", "timestamp")

      ratingsStringDF
        .withColumn("userId", toInt(ratingsStringDF("userId")))
        .withColumn("itemId", toInt(ratingsStringDF("itemId")))
        .withColumn("rating", toDouble(ratingsStringDF("rating")))
    }
    ratingsDF
  }
  def updatePredictionsForUser(userId: Int): DataFrame = {
    val predictions: DataFrame = calculatePredictionsForUser(userId, readModel())
    new File(String.format(collaborativePredictionsDirectoryPath, subRootDir)).mkdirs()
    dataUtil.persistPredictionsForUser(userId, predictions, String.format(collaborativePredictionsForUserPath, subRootDir, userId.toString))
    predictions
  }

  def persistPredictedIdsForUser(userId: Int, predictedItemIds: List[Int]): Unit = {
    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(String.format(predictionsPath, subRootDir), append = true)
    writer.writeRow(List(userId, predictedItemIds.toArray.mkString(":")))
  }

  def calculatePredictionsForUser(userId: Int, model: ALSModel): DataFrame = {
      val sparkSession = SparkUtil.sparkSession()
      import sparkSession.implicits._
    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS)
      .filter(not(isnan($"prediction")))
      .select($"itemId", $"prediction".as("rating"))
      .orderBy(col("rating").desc)
    predictions
  }

  def calculatePredictedIdsForUser(userId: Int, model: ALSModel): List[Int] = {
    val sparkSession = SparkUtil.sparkSession()
    import sparkSession.implicits._
    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS)
      .filter(not(isnan($"rating")))
      .orderBy(col("rating").desc)
    val predictedItemIds: List[Int] = predictions.select("itemId").map(row => row.getInt(0)).collect().toList
    predictedItemIds
  }

  def getUserMoviePairsToRate(userId: Int): DataFrame = {
    val sparkSession = SparkUtil.sparkSession()
    import sparkSession.implicits._
    val itemIDsNotRateByUser = dataUtil.getItemIDsNotRatedByUser(userId)
    val userMovieList: List[(Int, Int)] = itemIDsNotRateByUser.map(itemId => (userId, itemId))
    userMovieList.toDF("userId", "itemId")
  }
}

object CFPredictionServiceRunner extends App with Constants {

  Util.windowsWorkAround()
  val cfPredictionService = new CFPredictionService(mainSubDir)
  val dataUtil = new DataUtil(mainSubDir)

  // TODO remove!!!
  cfPredictionService.updatePredictionsForUser(777)

  /*
    Periodically run batch job which updates model
  */
  while(true) {
    Thread.sleep(1000)
    Util.tryAndLog(cfPredictionService.updateModel(), "Collaborative:: Updating model")
    val userIds: Set[Int] = dataUtil.getUserIdsFromLastNRatings(1000)
    for(userId <- userIds){
      Util.tryAndLog(cfPredictionService.updatePredictionsForUser(userId), "Collaborative:: Updating predictions for User " + userId)
    }
  }
}
