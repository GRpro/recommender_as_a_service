package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.sql.DataFrame

// TODO refactor this class
object CBPredictionService extends Constants{
  def main(args: Array[String]): Unit ={

    val userId: Int = 1
//    val pipeline = RandomForestEstimatorBuilder.build(userId) // TODO RF estimator needs to index all features first! Cannot pass random data in it!
    val pipeline = LinearRegressionWithElasticNetBuilder.build(userId)

    val spark = SparkUtil.sparkSession()
    val ratedItems = spark.read.format("libsvm")
      .load(String.format(ratingsWithFeaturesSVMPath, userId.toString))
    val notRatedItems = getItemsNotRateByUserSVM(userId)
    val model = pipeline.fit(ratedItems)
    val predictions = model.transform(notRatedItems)

    spark.stop()
  }

  def getItemsNotRateByUserSVM(userId: Int): DataFrame ={
    val spark = SparkUtil.sparkSession()
    import spark.implicits._
    val itemIdsNotRatedByUser = new PredictionService().getMoviesNotRatedByUser(userId)
    val list = itemIdsNotRatedByUser.select("movieId").rdd.map(r=>r(0)).collect()
    val notRatedItems = spark.read.format("libsvm") // TODO get really not rated items my the user
      .load(allMoviesSVMPath)
    val filtered = notRatedItems.filter($"label".isin(Seq(1,2,3))).show()
    spark.stop()
    notRatedItems

  }
}
