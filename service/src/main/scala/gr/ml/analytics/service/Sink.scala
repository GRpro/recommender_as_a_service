package gr.ml.analytics.service

import org.apache.spark.sql.DataFrame

trait Sink {
  /**
    * General method for storing predictions (CF, CB and final)
    *
    * @param predictions dataframe of (key: String, userId: Int, itemId: Int, prediction: float) triples
    */
  def storePredictions(predictions: DataFrame, table: String)

  /**
    * Persist popular items
    *
    */
  def persistPopularItems()

  def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int]): Unit
}
