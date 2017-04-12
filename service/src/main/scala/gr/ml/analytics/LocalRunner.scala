package gr.ml.analytics

import com.typesafe.config.ConfigFactory
import gr.ml.analytics.service.cf.CFJob
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
    val config = ConfigFactory.load("application.conf")

    val spark = getSparkSession

    val collaborativeFilteringJob = new CFJob(spark, config)

    do {
      collaborativeFilteringJob.run()

      Thread.sleep(INTERVAL_MS)

    } while(ITERATIVE)

  }
}
