package gr.ml.analytics.util

import java.io.{File, PrintWriter}

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.util.CSVtoSVMConverter.libsvmDirectory
import org.slf4j.LoggerFactory

object CSVtoSVMConverter extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  Util.windowsWorkAround()

  def createSVMRatingFilesForCurrentUsers(): Unit ={
    val userIds = new PredictionService().getUserIdsForPrediction()
    for(userId <- userIds){
      createSVMRatingsFileForUser(userId)
    }
  }

  def createSVMRatingsFileForUser(userId:Int): Unit ={
    val allGenres: List[String] = getAllGenres()
    val csvData = CSVReader.open(ratingsWithFeaturesPath).all()
    new File(libsvmDirectoryPath).mkdirs()
    val pw = new PrintWriter(new File(String.format(ratingsWithFeaturesSVMPath, userId.toString)))
    csvData.filter(r => r(0) == userId.toString)
      .foreach(r=>{
        var svmString: String = r(2)
        for(g <- allGenres)
          svmString += " " + (allGenres.indexOf(g)+1) +":"+ r(csvData(0).indexOf(g))
        pw.println(svmString)
      })
    pw.close()
  }

  def getAllGenres(): List[String] ={
    val allMovies = CSVReader.open(moviesPath).all().filter(p => p(0) != "movieId")

    val allGenres: List[String] = allMovies.map(l => l(2).replace("|", ":").split(":").toSet)
      .reduce((l1:Set[String],l2:Set[String])=>l1++l2)
      .filter(_ != "(no genres listed)")
      .toList.sorted
    allGenres
  }
  // Warning the "label" column will actually contain item ids!
  def createSVMFileForAllItems(): Unit ={
    val allGenres: List[String] = getAllGenres()
    val csvReader = CSVReader.open(moviesWithFeaturesPath)
    val pw = new PrintWriter(new File(allMoviesSVMPath))
    val csvData = csvReader.all()
    csvData.filter(r => r(0) != "movieId" )
      .foreach(r=>{
        var svmString: String = r(0)
        for(g <- allGenres)
          svmString += " " + (allGenres.indexOf(g)+1) +":"+ r(csvData(0).indexOf(g))
        pw.println(svmString)
      })
    pw.close()
    csvReader.close()
  }
}
