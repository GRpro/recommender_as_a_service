package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.util.{CSVtoSVMConverter, GenresFeatureEngineering, SparkUtil, Util}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

// TODO refactor this class
object CBPredictionService extends Constants {
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  def main(args: Array[String]): Unit = {

    // TODO check that all those commented out lines work fine

//    GenresFeatureEngineering.createAllRatingsWithFeaturesFile() // TODO it should be done just once at the application startup
//    GenresFeatureEngineering.createAllMoviesWithFeaturesFile()

//    CSVtoSVMConverter.createSVMFileForAllItems()
//    CSVtoSVMConverter.createSVMRatingFilesForCurrentUsers()

    Util.windowsWorkAround()
    val userIds = new PredictionService().getUserIdsForPrediction()

    for(userId <- userIds){
      val pipeline = LinearRegressionWithElasticNetBuilder.build(userId)
      //    val pipeline = RandomForestEstimatorBuilder.build(userId) // TODO RF estimator needs to index all features first! Cannot pass random data in it!
      Util.tryAndLog(updateModelForUser(pipeline, userId), "Content-based:: Updating model for user " + userId)
      Util.tryAndLog(updatePredictionsForUser(userId), "Content-based:: Updating predictions for User " + userId)
    }
  }

  def getItemsNotRateByUserSVM(userId: Int): DataFrame = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val doubleItemIDsNotRatedByUser = new PredictionService().getMovieIDsNotRatedByUser(userId).map(i => i.toDouble)
    val allItems = spark.read.format("libsvm").load(allMoviesSVMPath)
    val notRatedItems = allItems.filter($"label".isin(doubleItemIDsNotRatedByUser: _*))
    notRatedItems
  }

  def updateModelForUser(pipeline: Pipeline, userId: Int): Unit ={
    val spark = SparkUtil.sparkSession()
    val ratedItems = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, userId.toString))
    val model = pipeline.fit(ratedItems)
    model.write.overwrite().save(String.format(PredictionService.contentBasedModelForUserPath, userId.toString))
    spark.stop()
  }

  def updatePredictionsForUser(userId: Int): Unit ={
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val readModel = PipelineModel.load(String.format(PredictionService.contentBasedModelForUserPath, userId.toString))
    val notRatedItems = getItemsNotRateByUserSVM(userId)
    val predictions = readModel.transform(notRatedItems)
      .select($"label".as("movieId"), $"prediction")
      .sort($"prediction".desc)
    new PredictionService().persistPredictionsForUser(userId, predictions, String.format(contentBasedPredictionsForUserPath, userId.toString))
  }
}
