package gr.ml.analytics.util

import java.io.{File, PrintWriter}

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.Constants
import gr.ml.analytics.service.cf.CFPredictionService
import org.slf4j.LoggerFactory

object CSVtoSVMConverter extends App with Constants {

  val progressLogger = LoggerFactory.getLogger("progressLogger")

  Util.windowsWorkAround()

  def createSVMRatingFilesForAllUsers(): Unit ={
    val userIds = CFPredictionService.getAllUserIds()
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

  // WARNING!! the "label" column will actually contain item ids!
  def createSVMFileForAllItems(): Unit ={
    val allGenres: List[String] = GenresFeatureEngineering.getAllGenres()
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
