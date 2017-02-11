package gr.ml.analytics.model

import org.apache.spark.sql.SparkSession

object SparkUtil {

  def sparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark Movie Recommendation service")
      .getOrCreate
  }
}
