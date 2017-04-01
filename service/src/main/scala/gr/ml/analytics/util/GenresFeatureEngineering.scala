package gr.ml.analytics.util

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

object GenresFeatureEngineering extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  def getAllMovies(): List[List[String]] ={
    val moviesReader = CSVReader.open(moviesPath)
    val allMovies: List[List[String]] = moviesReader.all().filter(p => p(0) != "movieId")
    moviesReader.close()
    allMovies
  }

  def getAllGenres(): List[String] ={
    val allMovies = getAllMovies()
    val allGenres: List[String] = allMovies.map(l => l(2).replace("|", ":").split(":").toSet)
      .reduce((l1:Set[String],l2:Set[String])=>l1++l2)
      .filter(_ != "(no genres listed)")
      .toList.sorted
    allGenres
  }

  def createAllMoviesWithFeaturesFile(): Unit ={
    val moviesWithFeatures = getMoviesWithFeatures()

    val movieHeaderWriter = CSVWriter.open(moviesWithFeaturesPath, append = false)
    movieHeaderWriter.writeRow(moviesWithFeatures(0).map(t=>t._1).toList)
    movieHeaderWriter.close()
    val movieWriter = CSVWriter.open(moviesWithFeaturesPath, append = true)
    moviesWithFeatures.foreach(m => movieWriter.writeRow(m.map(t=>t._2).toList))
    movieWriter.close()
  }

  def createAllRatingsWithFeaturesFile(): Unit ={
    val moviesWithFeaturesById = getMoviesWithFeaturesById()
    val ratingsReader = CSVReader.open(ratingsPath)
    val allRatings = ratingsReader.all().filter(p => p(0) != "userId")

    val ratingsWithFeatures = allRatings.map((r:List[String])=>{
      val movieId = r(1)
      var mapToReturn = ListMap("userId" -> r(0), "movieId" -> movieId, "rating" -> r(2))
      mapToReturn ++ moviesWithFeaturesById.toMap.get(movieId).get
    })
    ratingsReader.close()
    val ratingsHeaderWriter = CSVWriter.open(ratingsWithFeaturesPath, append = false)
    ratingsHeaderWriter.writeRow(ratingsWithFeatures(0).map(t=>t._1).toList)
    ratingsHeaderWriter.close()
    val ratingsWriter = CSVWriter.open(ratingsWithFeaturesPath, append = true)
    ratingsWithFeatures.foreach(m => ratingsWriter.writeRow(m.map(t=>t._2).toList))
    ratingsWriter.close()
  }

  def getMoviesWithFeatures(): List[ListMap[String, String]] ={
    val allMovies = getAllMovies()
    val moviesWithFeatures = allMovies.map((p:List[String]) => {
      var mapToReturn = ListMap("movieId" -> p(0))
      val movieGenres = p(2).replace("|", ":").split(":")
      val allGenres: List[String] = getAllGenres()
      for(genre <- allGenres) {
        val containsThisGenre = if(movieGenres.contains(genre)) 1 else 0
        mapToReturn += (genre -> containsThisGenre.toString)
      }
      mapToReturn
    })
    moviesWithFeatures
  }

  def getMoviesWithFeaturesById(): List[(String, ListMap[String, String])] = {
    val allMovies = getAllMovies()
    val allGenres: List[String] = getAllGenres()
    val moviesWithFeaturesById = allMovies.map((p: List[String]) => {
      val movieId = p(0)
      var mapToReturn = ListMap("movieId" -> movieId)
      val movieGenres = p(2).replace("|", ":").split(":")
      for (genre <- allGenres) {
        val containsThisGenre = if (movieGenres.contains(genre)) 1 else 0
        mapToReturn += (genre -> containsThisGenre.toString)
      }
      (movieId -> mapToReturn)
    })
    moviesWithFeaturesById
  }
}
