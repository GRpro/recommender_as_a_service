package gr.ml.analytics.util

import java.nio.file.Paths

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import org.slf4j.LoggerFactory

import scala.collection.immutable.ListMap

object GenresFeatureEngineering extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  //TODO remove this and all related stuff. This is temporary fix for Windows
  if (System.getProperty("os.name").contains("Windows")) {
    val HADOOP_BIN_PATH = getClass.getClassLoader.getResource("").getPath
    System.setProperty("hadoop.home.dir", HADOOP_BIN_PATH)
  }

  Util.loadResource(smallDatasetUrl,
    Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
  Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
    Paths.get(datasetsDirectory).toAbsolutePath)


  val allMovies = CSVReader.open(moviesPath).all().filter(p => p(0) != "movieId")

  val allGenres: List[String] = allMovies.map(l => l(2).replace("|", ":").split(":").toSet)
    .reduce((l1:Set[String],l2:Set[String])=>l1++l2)
    .filter(_ != "(no genres listed)")
    .toList.sorted

  val moviesWithFeatures = getMoviesWithFeatures()
  val moviesWithFeaturesById = getMoviesWithFeaturesById()

  val movieHeaderWriter = CSVWriter.open(moviesWithFeaturesPath, append = false)
  movieHeaderWriter.writeRow(moviesWithFeatures(0).map(t=>t._1).toList)
  movieHeaderWriter.close()
  val movieWriter = CSVWriter.open(moviesWithFeaturesPath, append = true)
  moviesWithFeatures.foreach(m => movieWriter.writeRow(m.map(t=>t._2).toList))
  movieWriter.close()

  // TODO caution, this is not all the data:
  val allRatings = CSVReader.open(ratingsSmallPath).all().filter(p => p(0) != "userId")

  val ratingsWithFeatures = allRatings.map((r:List[String])=>{
    val movieId = r(1)
    var mapToReturn = ListMap("userId" -> r(0), "movieId" -> movieId, "rating" -> r(2))
    mapToReturn ++ moviesWithFeaturesById.toMap.get(movieId).get
  })

  val ratingsHeaderWriter = CSVWriter.open(ratingsWithFeaturesPath, append = false)
  ratingsHeaderWriter.writeRow(ratingsWithFeatures(0).map(t=>t._1).toList)
  ratingsHeaderWriter.close()
  val ratingsWriter = CSVWriter.open(ratingsWithFeaturesPath, append = true)
  ratingsWithFeatures.foreach(m => ratingsWriter.writeRow(m.map(t=>t._2).toList))
  ratingsWriter.close()

  def getMoviesWithFeatures(): List[ListMap[String, String]] ={
    val moviesWithFeatures = allMovies.map((p:List[String]) => { // TODO collect as a map with movieId as a key
    val movieId = p(0)
      var mapToReturn = ListMap("title" -> p(1)) // TODO do I need title here?
      val movieGenres = p(2).replace("|", ":").split(":")
      for(genre <- allGenres) {
        val containsThisGenre = if(movieGenres.contains(genre)) 1 else 0
        mapToReturn += (genre -> containsThisGenre.toString)
      }
      mapToReturn
    })
    moviesWithFeatures
  }

  def getMoviesWithFeaturesById(): List[(String, ListMap[String, String])] ={
    val moviesWithFeaturesById = allMovies.map((p:List[String]) => { // TODO collect as a map with movieId as a key
    val movieId = p(0)
      var mapToReturn = ListMap("title" -> p(1)) // TODO do I need title here?
      val movieGenres = p(2).replace("|", ":").split(":")
      for(genre <- allGenres) {
        val containsThisGenre = if(movieGenres.contains(genre)) 1 else 0
        mapToReturn += (genre -> containsThisGenre.toString)
      }
      (movieId->mapToReturn)
    })
    moviesWithFeaturesById
  }
}
