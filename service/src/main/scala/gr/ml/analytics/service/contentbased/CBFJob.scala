package gr.ml.analytics.service.contentbased

import com.typesafe.config.Config
import gr.ml.analytics.service.{CassandraSink, CassandraSource, Sink, Source}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

class CBFJob(val sparkSession: SparkSession,
             val source: Source,
             val sink: Sink,
             val params: Map[String, Any],
             pipeline: => Pipeline) {

  private val ONE_DAY = 24 * 3600
  private val MINIMAL_POSITIVE_RATING = 3

  import sparkSession.implicits._

  def run(): Unit = {

    val itemAndFeaturesDF = source.getAllItemsAndFeatures()

    for (userId <- source.getUserIdsForLastNSeconds(ONE_DAY)) {
      // each user requires a separate model
      // CBF steps:
      // 1. select DataFrame of (label, features) for a given user
      // 2. train model using dataset from step 1
      // 3. get not rated items
      // 4. perform predictions using created model

      // TODO Slow. Improve performance
      val trainingDF = source.all
        .filter($"userid" === userId)
        .select("itemid", "rating")
        .as("d1").join(itemAndFeaturesDF.as("d2"), $"d1.itemid" === $"d2.itemid")
        .select($"d1.rating".as("label"),
          $"d2.features".as("features"))

      trainingDF.show()

      val model = pipeline.fit(trainingDF)

      val notRatedDF = source.getUserItemPairsToRate(userId)
        .as("d1").join(itemAndFeaturesDF.as("d2"), $"d1.itemId" === $"d2.itemid")
        .select($"d1.itemId".as("itemId"), $"d1.userId".as("userId"),
          $"d2.features".as("features"))

      notRatedDF.show() // TODO remove

      val predictedRatingsDS = model.transform(notRatedDF)
        .filter(col("prediction").isNotNull)
        .filter(s"prediction > $MINIMAL_POSITIVE_RATING") // TODO probably remove this
        .select("userId", "itemId", "prediction")

      predictedRatingsDS.show() // TODO remove

      sink.storeCBPredictions(predictedRatingsDS)
    }
  }
}


object CBFJob {

  def apply(sparkSession: SparkSession,
            config: Config,
            sourceOption: Option[Source],
            sinkOption: Option[Sink],
            params: Map[String, Any]
           ): CBFJob = {

    val source = sourceOption match {
      case Some(s) => s
      case None => new CassandraSource(sparkSession, config)
    }
    val sink = sinkOption match {
      case Some(s) => s
      case None => new CassandraSink(sparkSession, config)
    }

    lazy val pipeline = LinearRegressionWithElasticNetBuilder.build("")

    new CBFJob(sparkSession, source, sink, params, pipeline)
  }


}