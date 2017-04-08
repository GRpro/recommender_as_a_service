package gr.ml.analytics.service

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.util.CSVtoSVMConverter

class RecommenderServiceImpl extends RecommenderService with Constants {

  /**
    * @inheritdoc
    */
  override def save(userId: Int, itemId: Int, rating: Double): Unit = {
    val writer = CSVWriter.open(ratingsPath, append = true)
    writer.writeRow(List(userId.toString, itemId.toString,rating.toString, (System.currentTimeMillis / 1000).toString))
    CSVtoSVMConverter.createSVMRatingsFileForUser(userId)
  }

  /**
    * @inheritdoc
    */
  override def getTop(userId: Int, n: Int): List[Int] = {
    val predictionsReader = CSVReader.open(predictionsPath)
    val allPredictions = predictionsReader.all()
    predictionsReader.close()
    val filtered = allPredictions.filter((pr: List[String]) => pr.head.toInt == userId)
    if (filtered.size > 0) {
      val predictedItemIdsFromFile = filtered.last(1).split(":").toList.map(m => m.toInt).take(n)
      predictedItemIdsFromFile
    }
    else {
      val popularItemsReader = CSVReader.open(popularItemsPath)
      val popularItemIds = popularItemsReader.all().filter(l => l(0) != "itemId").map(l => l(2).toInt).take(n)
      popularItemsReader.close()
      popularItemIds
    }
  }
}
