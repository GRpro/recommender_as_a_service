package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.service._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val config: Config,
            val source: Source,
            val sink: Sink,
            val params: Map[String, Any])(implicit val sparkSession: SparkSession) {

  private val ONE_DAY = 24 * 3600   // TODO Set it from redis
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")


  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.all.select("userId", "itemId", "rating")

    val rank = params("cf_rank").toString.toInt
    val regParam = params("cf_reg_param").toString.toDouble
    val als = new ALS()
      .setMaxIter(2)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    for(userId <- source.getUserIdsForLastNSeconds(ONE_DAY)){
     val notRatedPairsDF = source.getUserItemPairsToRate(userId)
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull)
        .select("userid", "itemid", "prediction")
        .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))

      sink.storePredictions(predictedRatingsDS, cfPredictionsTable)
    }
  }
}

object CFJob extends Constants {

  def apply(config: Config,
            source: Source,
            sink: Sink,
            params: Map[String, Any])(implicit sparkSession: SparkSession): CFJob = {

    new CFJob(config, source, sink, params)
  }

  private def readModel(spark: SparkSession): ALSModel = {
    val model = ALSModel.load(String.format(collaborativeModelPath, mainSubDir))
    model
  }

  private def writeModel(spark: SparkSession, model: ALSModel): ALSModel = {
    model.write.overwrite().save(String.format(collaborativeModelPath, mainSubDir))
    model
  }
}
