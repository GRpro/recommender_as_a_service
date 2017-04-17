package gr.ml.analytics.util_old

import org.apache.spark.sql.SparkSession

object SparkUtil {

  def sparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Spark Movie Recommendation service")
      .getOrCreate
  }
}
