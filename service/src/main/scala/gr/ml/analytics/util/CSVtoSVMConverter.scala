package gr.ml.analytics.util

import java.io.{File, PrintWriter}

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.PredictionService
import org.slf4j.LoggerFactory

object CSVtoSVMConverter extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  Util.windowsWorkAround()

  def createSVMRatingFilesForAllUsers(): Unit ={
    val userIds = new PredictionService().getAllUserIds()
    for(userId <- userIds){
      createSVMRatingsFileForUser(userId)
    }
  }

  def createSVMRatingsFileForUser(userId:Int): Unit ={
    println("createSVMRatingsFileForUser :: UserID = " + userId)
    val itemsReader = CSVReader.open(moviesWithFeaturesPath)
    val ratingsReader = CSVReader.open(ratingsPath)
    val allItems = itemsReader.all()
    val allRatings = ratingsReader.all()
    new File(libsvmDirectoryPath).mkdirs()
    val pw = new PrintWriter(new File(String.format(ratingsWithFeaturesSVMPath, userId.toString)))
    allRatings.filter(r => r(0) == userId.toString)
      .foreach(r=>{
        val movieId = r(1)
        var svmString: String = movieId
        val featuresList = allItems.filter(l=>l(0)==movieId)(0).drop(1)
        featuresList.zipWithIndex.foreach(t=>svmString+=(" "+(t._2+1)+":"+t._1))
        pw.println(svmString)
      })
    pw.close()
    itemsReader.close()
    ratingsReader.close()
  }

  def getAllGenres(): List[String] ={
    val allMovies = CSVReader.open(moviesPath).all().filter(p => p(0) != "movieId")

    val allGenres: List[String] = allMovies.map(l => l(2).replace("|", ":").split(":").toSet)
      .reduce((l1:Set[String],l2:Set[String])=>l1++l2)
      .filter(_ != "(no genres listed)")
      .toList.sorted
    allGenres
  }
  // WARNING!! the "label" column will actually contain item ids!
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
        println("createSVMFileForAllItems :: MovieID = "  +r(0))
      })
    pw.close()
    csvReader.close()
  }
}
