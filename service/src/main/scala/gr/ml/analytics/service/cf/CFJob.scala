package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.service._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val sparkSession: SparkSession,
            val source: Source,
            val sink: Sink,
            val params: Map[String, Any]) {

  private val ONE_DAY = 24 * 3600

  private val minimumPositiveRating = 3.0

  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.getAllRatings.select("userId", "itemId", "rating")

    val rank = params("rank").toString.toInt
    val regParam = params("reg_param").toString.toDouble
    val als = new ALS()
      .setMaxIter(2)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    for (userId <- source.getUserIdsForLast(ONE_DAY)) {
      val notRatedPairsDF = source.getUserItemPairsToRate(userId)
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull)
        .filter(s"prediction > $minimumPositiveRating")
        .select("userId", "itemId", "prediction")

      sink.store(predictedRatingsDS)
    }
  }
}

object CFJob extends Constants {

  def apply(sparkSession: SparkSession,
            config: Config,
            sourceOption: Option[Source],
            sinkOption: Option[Sink],
            params: Map[String, Any]): CFJob = {
    val source = sourceOption match {
      case Some(s) => s
      case None => new CassandraSource(sparkSession, config)
    }
    val sink = sinkOption match {
      case Some(s) => s
      case None => new CassandraSink(sparkSession, config, "cf_predictions")
    }
    new CFJob(sparkSession, source, sink, params)
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
