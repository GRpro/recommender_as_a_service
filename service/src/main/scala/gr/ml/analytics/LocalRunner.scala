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
    val params = paramsStorage.getParams()

//    val cfJob = CFJob(spark, config, None, None, params)
    val cbfJob = CBFJob(spark, config, None, None, params)

    do {
//      cfJob.run()
      cbfJob.run()

      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)

  }
}
