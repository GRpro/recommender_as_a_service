package gr.ml.analytics.service.contentbased

import java.io.File

import gr.ml.analytics.service.Constants
import gr.ml.analytics.util.{DataUtil, SparkUtil, Util}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class CBPredictionService(subRootDir: String) extends Constants {
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)
  val dataUtil = new DataUtil(subRootDir)


  def main(args: Array[String]): Unit = {

    Util.windowsWorkAround()
    val userIds = dataUtil.getUserIdsFromLastNRatings(1000)

    for(userId <- userIds){
//      val pipeline = LinearRegressionWithElasticNetBuilder.build(subRootDir)
          val pipeline = RandomForestEstimatorBuilder.build(subRootDir)
      Util.tryAndLog(updateModelForUser(pipeline, userId), "Content-based:: Updating model for user " + userId)
      Util.tryAndLog(updatePredictionsForUser(userId), "Content-based:: Updating predictions for User " + userId)
    }
  }

  def getItemsNotRateByUserSVM(userId: Int): DataFrame = {
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val doubleItemIDsNotRatedByUser = dataUtil.getItemIDsNotRatedByUser(userId).map(i => i.toDouble)
    val allItems = spark.read.format("libsvm").load(String.format(allMoviesSVMPath, subRootDir))
    val notRatedItems = allItems.filter($"label".isin(doubleItemIDsNotRatedByUser: _*))
    notRatedItems
  }

  def updateModelForUser(pipeline: Pipeline, userId: Int): Unit ={
    val spark = SparkUtil.sparkSession()
    val ratedItems = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, subRootDir, userId.toString))
    val model = pipeline.fit(ratedItems)
    model.write.overwrite().save(String.format(contentBasedModelForUserPath, subRootDir, userId.toString))
    spark.stop()
  }

  def updatePredictionsForUser(userId: Int): Unit ={
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val readModel = PipelineModel.load(String.format(contentBasedModelForUserPath, subRootDir, userId.toString))
    val notRatedItems = getItemsNotRateByUserSVM(userId)
    val predictions = readModel.transform(notRatedItems)
      .select($"label".as("itemId"), $"prediction".as("rating"))
      .sort($"rating".desc)
    new File(String.format(contentBasedPredictionsDirectoryPath, subRootDir)).mkdirs()
    dataUtil.persistPredictionsForUser(userId, predictions,
      String.format(contentBasedPredictionsForUserPath, subRootDir, userId.toString))
  }
}