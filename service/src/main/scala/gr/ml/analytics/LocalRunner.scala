package gr.ml.analytics

import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service._
import gr.ml.analytics.service.HybridServiceRunner.mainSubDir
import gr.ml.analytics.service.cf.CFJob
import gr.ml.analytics.util.RedisParamsStorage
import gr.ml.analytics.service.contentbased.{CBFJob, LinearRegressionWithElasticNetBuilder}
import gr.ml.analytics.service.popular.PopularItemsJob
import gr.ml.analytics.util.{ParamsStorage, Util}
import org.apache.spark.sql.SparkSession

/**
  * Run Spark jobs in local mode periodically.
  */
object LocalRunner {

  val ITERATIVE = false
  val INTERVAL_MS = 5000

  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark Recommendation Service")
      .getOrCreate
  }


  def main(args: Array[String]): Unit = {

    Util.windowsWorkAround()

    val config = ConfigFactory.load("application.conf")

    val schemaId = 0

    val paramsStorage: ParamsStorage = new RedisParamsStorage

    implicit val spark = getSparkSession

    val params = paramsStorage.getParams()

    // val featureExtractor = new RowFeatureExtractor

    // function to get feature ration from Redis
    def getRatio(featureName: String): Double = {
      val itemFeatureRationPattern = "schema.%s.item.feature.%s.ratio"
      paramsStorage.getParam(itemFeatureRationPattern.format(schemaId, featureName))
        .toString.toDouble
    }

    val featureExtractor = new WeightedFeatureExtractor(getRatio)

    val pipeline = LinearRegressionWithElasticNetBuilder.build("")

    val source = new CassandraSource(config, featureExtractor)
    val sink = new CassandraSink(config)

    val cfJob = CFJob(config, source, sink, params)
    val cbfJob = CBFJob(config, source, sink, pipeline, params)
    val popularItemsJob = PopularItemsJob(source, config)

    val hb = new HybridService(mainSubDir, config, source, sink, paramsStorage)

    do {

      cfJob.run()
      cbfJob.run()
      popularItemsJob.run()

      hb.combinePredictionsForLastUsers(0.1)
      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)

  }
}
