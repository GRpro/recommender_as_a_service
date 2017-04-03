package gr.ml.analytics.service

import com.github.tototoshi.csv._
import gr.ml.analytics.util.{CSVtoSVMConverter, GenresFeatureEngineering}
class RecommenderServiceImpl extends RecommenderService with Constants {

  /**
    * @inheritdoc
    */
  override def save(userId: Int, movieId: Int, rating: Double): Unit = {
    val writer = CSVWriter.open(ratingsPath, append = true)
    writer.writeRow(List(userId.toString, movieId.toString,rating.toString, (System.currentTimeMillis / 1000).toString))
    GenresFeatureEngineering.addRatingToRatingsWithFeatures(userId, movieId, rating)
    CSVtoSVMConverter.createSVMRatingsFileForUser(userId)
  }

  /**
    * @inheritdoc
    */
  override def getTop(userId: Int, n: Int): List[Int] = {
    val reader = CSVReader.open(predictionsPath)
    val filtered = reader.all().filter((pr: List[String]) => pr.head.toInt == userId).last
    val predictedMovieIdsFromFile = filtered(1).split(":").toList.map(m => m.toInt).take(n)
    reader.close()
    predictedMovieIdsFromFile
  }
}
