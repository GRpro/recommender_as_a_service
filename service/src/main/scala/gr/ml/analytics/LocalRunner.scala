package gr.ml.analytics

import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service.cf.CFJob
import gr.ml.analytics.util.{ParamsStorage, RedisParamsStorage, Util}
import gr.ml.analytics.service.contentbased.CBFJob
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
    val paramsStorage: ParamsStorage = new RedisParamsStorage
    val spark = getSparkSession
    val cfParams = paramsStorage.getCFParams()
    val cfJob = CFJob(spark, config, None, None, cfParams)

//    val cfParams = ParamsStorage.getCFParams()
    val cbfParams = ParamsStorage.getCFParams()

//    val cfJob = CFJob(spark, config, None, None, cfParams)
    val cbfJob = CBFJob(spark, config, None, None, cbfParams)
    do {
//      cfJob.run()
      cbfJob.run()

      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)

  }
}
