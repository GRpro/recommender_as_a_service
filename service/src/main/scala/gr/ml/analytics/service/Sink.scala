package gr.ml.analytics.service

import org.apache.spark.sql.DataFrame

trait Sink {
  /**
    * Store predictions dataframe
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: float) triples
    */
  def store(predictions: DataFrame)
}
