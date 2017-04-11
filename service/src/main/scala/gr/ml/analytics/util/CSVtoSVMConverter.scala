package gr.ml.analytics.util

import java.io.{File, PrintWriter}

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.service.Constants
import org.slf4j.LoggerFactory

class CSVtoSVMConverter(subRootDir: String) extends Constants {

  val dataUtil = new DataUtil(subRootDir)

  val progressLogger = LoggerFactory.getLogger("progressLogger")

  Util.windowsWorkAround()

  def createSVMRatingFilesForAllUsers(): Unit ={
    val userIds = dataUtil.getAllUserIds()
    for(userId <- userIds){
      createSVMRatingsFileForUser(userId)
    }
  }

  def createSVMRatingsFileForUser(userId:Int): Unit ={
    println("createSVMRatingsFileForUser :: UserID = " + userId)
    val itemsReader = CSVReader.open(String.format(moviesWithFeaturesPath, subRootDir))
    val ratingsReader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val allItems = itemsReader.all()
    val allRatings = ratingsReader.all()
    new File(String.format(libsvmDirectoryPath, subRootDir)).mkdirs()
    val pw = new PrintWriter(new File(String.format(ratingsWithFeaturesSVMPath, subRootDir, userId.toString)))
    allRatings.filter(r => r(0) == userId.toString)
      .foreach(r=>{
        val itemId = r(1)
        val rating = r(2)
        var svmString: String = rating
        val featuresList = allItems.filter(l=>l(0)==itemId)(0).drop(1)
        featuresList.zipWithIndex.foreach(t=>svmString+=(" "+(t._2+1)+":"+t._1))
        pw.println(svmString)
      })
    pw.close()
    itemsReader.close()
    ratingsReader.close()
  }

  // WARNING!! the "label" column will actually contain item ids!
  def createSVMFileForAllItems(): Unit ={
    val allGenres: List[String] = new GenresFeatureEngineering(subRootDir).getAllGenres()
    val csvReader = CSVReader.open(String.format(moviesWithFeaturesPath, subRootDir))
    val pw = new PrintWriter(new File(String.format(allMoviesSVMPath, subRootDir)))
    val csvData = csvReader.all()
    csvData.filter(r => r(0) != "itemId" )
      .foreach(r=>{
        var svmString: String = r(0)
        for(g <- allGenres)
          svmString += " " + (allGenres.indexOf(g)+1) +":"+ r(csvData(0).indexOf(g))
        pw.println(svmString)
        println("createSVMFileForAllItems :: ItemID = "  +r(0))
      })
    pw.close()
    csvReader.close()
  }
}
