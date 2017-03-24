package gr.ml.analytics

import java.nio.file.Paths

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.model.Util
import gr.ml.analytics.service.PredictionService
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


  val allRows = CSVReader.open(moviesPath).all().filter(p => p(0) != "movieId")

  val allGenres: List[String] = allRows.map(l => l(2).replace("|", ":").split(":").toSet)
    .reduce((l1:Set[String],l2:Set[String])=>l1++l2)
    .filter(_ != "(no genres listed)")
    .toList.sorted

  val moviesWithFeatures = allRows.map((p:List[String]) => {
    var mapToReturn = ListMap("movieId" -> p(0), "title" -> p(1))
    val movieGenres = p(2).replace("|", ":").split(":")
    for(genre <- allGenres) {
      val containsThisGenre = if(movieGenres.contains(genre)) 1 else 0
            mapToReturn += (genre -> containsThisGenre.toString)
    }
    mapToReturn
  })
  val writer = CSVWriter.open(moviesWithFeaturesPath, append = true)
  writer.writeRow(moviesWithFeatures(0).map(t=>t._1).toList)
  moviesWithFeatures.foreach(m => writer.writeRow(m.map(t=>t._2).toList))
}
