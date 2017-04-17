package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.Rating

class RecommenderServiceImpl(inputDatabase: InputDatabase) extends RecommenderService with LazyLogging {

  private lazy val ratingModel = inputDatabase.ratingModel

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double, timestamp: Long): Unit = {
    val ratingEntity = Rating(userId, itemId, rating, timestamp)
    ratingModel.save(ratingEntity)

    logger.info(s"saved $ratingEntity")

  }

  /**
    * @inheritdoc
    */
  // TODO implement when migrate to cassandra
  override def getTop(userId: Int, n: Int): List[Int] = {
//    val predictionsReader = CSVReader.open(predictionsPath)
//    val allPredictions = predictionsReader.all()
//    predictionsReader.close()
//    val filtered = allPredictions.filter((pr: List[String]) => pr.head.toInt == userId)
//    if (filtered.size > 0) {
//      val predictedItemIdsFromFile = filtered.last(1).split(":").toList.map(m => m.toInt).take(n)
//      predictedItemIdsFromFile
//    }
//    else {
//      val popularItemsReader = CSVReader.open(popularItemsPath)
//      val popularItemIds = popularItemsReader.all().filter(l => l(0) != "itemId").map(l => l(2).toInt).take(n)
//      popularItemsReader.close()
//      popularItemIds
//    }
    List()
  }
}
