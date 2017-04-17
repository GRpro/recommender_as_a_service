package gr.ml.analytics.util_old

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.Constants
import org.apache.spark.sql.DataFrame

class DataUtil(val subRootDir: String) extends Constants{
  def getItemIDsNotRatedByUser(userId: Int): List[Int] = {
    val ratingsReader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val allRatings = ratingsReader.all()
    ratingsReader.close()

    val allMovieIDs = getAllItemIDs()
    val movieIdsRatedByUser = allRatings.filter((p:List[String])=>p(1)!="itemId" && p(0).toInt==userId)
      .map((p:List[String]) => p(1).toInt).toSet
    val movieIDsNotRateByUser = allMovieIDs.filter(m => !movieIdsRatedByUser.contains(m))
    movieIDsNotRateByUser
  }

  def getAllItemIDs(): List[Int] ={
    val reader = CSVReader.open(String.format(moviesPath, subRootDir))
    val allItemIds = reader.all().filter(r=>r(0)!="movieId").map(r=>r(0).toInt) // in MovieLens dataset it is movieId not itemId
    reader.close()
    allItemIds
  }

  def persistPredictionsForUser(userId: Int, predictions: DataFrame, path: String): Unit = {
    val predictionsHeaderWriter = CSVWriter.open(path, append = false)
    predictionsHeaderWriter.writeRow(List("userId","itemId","rating"))
    predictionsHeaderWriter.close()

    val predictionsWriter = CSVWriter.open(String.format(path), append = true)
    val predictionsList = predictions.rdd.map(r=>List(userId,r(r.fieldIndex("itemId")).toString.toDouble.toInt,r(r.fieldIndex("rating")))).collect()
    predictionsWriter.writeAll(predictionsList)
    predictionsWriter.close()
  }

  def getUserIdsFromLastNRatings(lastN: Int): Set[Int] = {
    val reader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val allUserIds = reader.all().filter(r => r(0) != "userId").map(r => r(0).toInt)
    val recentIds = allUserIds.splitAt(allUserIds.size - lastN)._2.toSet
    reader.close()
    recentIds
  }

  def getAllUserIds(): Set[Int] ={
    val reader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val allUserIds = reader.all().filter(r=>r(0)!="userId").map(r=>r(0).toInt).toSet
    reader.close()
    allUserIds
  }
}
