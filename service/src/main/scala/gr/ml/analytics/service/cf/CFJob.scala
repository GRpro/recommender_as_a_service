package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.service._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val sparkSession: SparkSession,
            val config: Config,
            val source: Source,
            val sink: Sink,
            val params: Map[String, Any]) {

  private val ONE_DAY = 24 * 3600   // TODO Set it from redis
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")


  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.all.select("userId", "itemId", "rating")

    val rank = params.get("cf_rank").get.toString.toInt
    val regParam = params.get("cf_reg_param").get.toString.toDouble
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
      case None => new CassandraSink(sparkSession, config)
    }
    new CFJob(sparkSession, config, source, sink, params)
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
