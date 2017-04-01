package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

// TODO refactor this class
object CBPredictionService extends Constants{
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  def main(args: Array[String]): Unit ={

    //CSVtoSVMConverter.createSVMFileForAllItems()

    val userId: Int = 1
//    val pipeline = RandomForestEstimatorBuilder.build(userId) // TODO RF estimator needs to index all features first! Cannot pass random data in it!
    val pipeline = LinearRegressionWithElasticNetBuilder.build(userId)

    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val ratedItems = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, userId.toString))
    val notRatedItems = getItemsNotRateByUserSVM(userId)
    val model = pipeline.fit(ratedItems)
    val predictions = model.transform(notRatedItems)
      .select($"label".as("movieId"), $"prediction")
      .sort($"prediction".desc)
    new PredictionService().persistPredictionsForUser(userId, predictions, String.format(contentBasedPredictionsForUserPath, userId.toString))

    spark.stop()
  }

  def getItemsNotRateByUserSVM(userId: Int): DataFrame ={
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val doubleItemIDsNotRatedByUser = new PredictionService().getMovieIDsNotRatedByUser(userId).map(i=>i.toDouble)
    val allItems = spark.read.format("libsvm").load(allMoviesSVMPath)
    val notRatedItems = allItems.filter($"label".isin(doubleItemIDsNotRatedByUser:_*))
    notRatedItems
  }
}
