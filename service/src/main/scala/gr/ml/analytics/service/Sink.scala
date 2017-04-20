package gr.ml.analytics.service

import org.apache.spark.sql.DataFrame

trait Sink {
  /**
    * Store CB predictions
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: float) triples
    */
  def storeCBPredictions(predictions: DataFrame)

  /**
    * Store CF predictions
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: float) triples
    */
  def storeCFPredictions(predictions: DataFrame)

  /**
    * Persist popular items
    *
    */
  def persistPopularItems()
}
