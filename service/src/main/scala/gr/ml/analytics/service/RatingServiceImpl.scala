package gr.ml.analytics.service

import com.github.tototoshi.csv._
import gr.ml.analytics.Constants

class RatingServiceImpl extends RatingService with Constants {

  /**
    * Create new ratings for a given user
    *
    * @param userId id of the user who rated movies
    * @param movieId id of the movie rated by user
    * @param rating users rating of the movie
    */
  override def save(userId: Int, movieId: Int, rating: Double): Unit = {
    val writer = CSVWriter.open(currentRatingsPath, append = true)
    writer.writeRow(List(userId.toString, movieId.toString,rating.toString,(System.currentTimeMillis/1000).toString))
  }

  /**
    * Get most relevant movies for a given user
    * @param userId id of the user to get recommendations for
    * @param n number of recommendation ids to be returned
    * @return ordered list of movie ids
    */
  override def getTop(userId: Int, n: Int): List[Int] = {
    val reader = CSVReader.open(predictionsPath)
    val filtered = reader.all().filter((pr: List[String]) => pr(0).toInt == userId)
    val predictedMovieIdsFromFile = filtered.map((pr: List[String]) => pr(1).split(":").toList.map(m => m.toInt)).last.take(n)
    predictedMovieIdsFromFile
  }
}
