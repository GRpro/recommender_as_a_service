package gr.ml.analytics.service

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.cf.CFPredictionService
import gr.ml.analytics.service.contentbased.{CBPredictionService, GeneralizedLinearRegressionBuilder, LinearRegressionWithElasticNetBuilder, RandomForestEstimatorBuilder}
import gr.ml.analytics.util.{CSVtoSVMConverter, DataUtil, GenresFeatureEngineering, Util}
import org.apache.spark.ml.Pipeline
import org.slf4j.LoggerFactory

class HybridService(subRootDir: String, lastNRatings: Int, collaborativeWeight: Double, contentBasedWeight: Double) extends Constants{

  val dataUtil = new DataUtil(subRootDir)
  val cfPredictionService = new CFPredictionService(subRootDir)
  val cbPredictionService = new CBPredictionService(subRootDir)
  val csv2svmConverter = new CSVtoSVMConverter(subRootDir)

  def run(cbPipeline: Pipeline): Unit ={
    prepareNecessaryFiles()

    while(true){
      Thread.sleep(5000) // can be increased for production
      runOneCycle(cbPipeline)
      combinePredictionsForLastUsers(collaborativeWeight, contentBasedWeight)
    }

  }

  def prepareNecessaryFiles(): Unit ={
    val startTime = System.currentTimeMillis()
    if(!(new File(String.format(moviesWithFeaturesPath, subRootDir)).exists()))
      new GenresFeatureEngineering(subRootDir).createAllMoviesWithFeaturesFile()
    val userIds = dataUtil.getUserIdsFromLastNRatings(lastNRatings)
    userIds.foreach(csv2svmConverter.createSVMRatingsFileForUser)
    if(!(new File(allMoviesSVMPath).exists()))
      csv2svmConverter.createSVMFileForAllItems()
    cfPredictionService.persistPopularItemIDS()
    val finishTime = System.currentTimeMillis()

    LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: Startup time took: " + (finishTime - startTime) + " millis.")
  }

  def runOneCycle(cbPipeline: Pipeline): Unit ={
    Util.tryAndLog(cfPredictionService.updateModel(), subRootDir + " :: Collaborative:: Updating model")
    val userIds = dataUtil.getUserIdsFromLastNRatings(lastNRatings)
    for(userId <- userIds){
      Util.tryAndLog(cbPredictionService.updateModelForUser(cbPipeline, userId), subRootDir + " :: Content-based:: Updating model for user " + userId)
      Util.tryAndLog(cfPredictionService.updatePredictionsForUser(userId), subRootDir + " :: Collaborative:: Updating predictions for User " + userId)
      Util.tryAndLog(cbPredictionService.updatePredictionsForUser(userId), subRootDir + " :: Content-based:: Updating predictions for User " + userId)
      LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: ##################### END OF ITERATION ##########################")
    }
  }

  def multiplyByWeight(predictions: List[List[String]], weight: Double): List[List[String]] ={
    predictions.map(l=>List(l(0), l(1), (l(2).toDouble * weight).toString))
  }

  def combinePredictionsForLastUsers(collaborativeWeight: Double, contentBasedWeight: Double): Unit ={
    val userIds = dataUtil.getUserIdsFromLastNRatings(lastNRatings)
    for(userId <- userIds){
//      Util.tryAndLog(combinePredictionsForUser(userId, collaborativeWeight, contentBasedWeight), subRootDir + " :: Hybrid:: Combining CF and CB predictions for user " + userId)
      combinePredictionsForUser(userId, collaborativeWeight, contentBasedWeight)
    }
  }

  def combinePredictionsForUser(userId: Int, collaborativeWeight: Double, contentBasedWeight: Double): Unit ={
    new File(String.format(finalPredictionsDirectoryPath, subRootDir)).mkdirs()
    val collaborativeReader = CSVReader.open(String.format(collaborativePredictionsForUserPath, subRootDir, userId.toString))
    val collaborativePredictions = collaborativeReader.all().filter(l=>l(0)!="userId")
    collaborativeReader.close()
    val contentBasedReader = CSVReader.open(String.format(contentBasedPredictionsForUserPath, subRootDir, userId.toString))
    val contentBasedPredictions = contentBasedReader.all().filter(l=>l(0)!="userId")
    contentBasedReader.close()

    val weightedCollaborativePredictions = multiplyByWeight(collaborativePredictions, collaborativeWeight)
    val weightedContentBasedPredictions = multiplyByWeight(contentBasedPredictions, contentBasedWeight)

    val allPredictions= weightedCollaborativePredictions ++ weightedContentBasedPredictions

    val hybridPredictions: List[List[String]]= allPredictions.groupBy(l=>l(1))
      .map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble+l2(2).toDouble).toString))))
      .map(t=>t._2).toList.sortWith((l,r) => l(2).toDouble > r(2).toDouble)

    val finalPredictionsHeaderWriter = CSVWriter.open(String.format(finalPredictionsForUserPath, subRootDir, userId.toString), append = false)
    finalPredictionsHeaderWriter.writeRow(List("userId", "itemId", "rating"))
    finalPredictionsHeaderWriter.close()
    val finalPredictionsWriter = CSVWriter.open(String.format(finalPredictionsForUserPath, subRootDir, userId.toString), append = true)
    finalPredictionsWriter.writeAll(hybridPredictions)
    finalPredictionsWriter.close()

    val finalPredictedIDs = hybridPredictions.map(l=>l(1).toInt)
    cfPredictionService.persistPredictedIdsForUser(userId, finalPredictedIDs)
  }
}

object HybridServiceRunner extends App with Constants{
  Util.loadAndUnzip(mainSubDir) // TODO new ratings will be rewritten!!
  //      val cbPipeline = LinearRegressionWithElasticNetBuilder.build(userId)
  val cbPipeline = RandomForestEstimatorBuilder.build(mainSubDir)
  //      val cbPipeline = GeneralizedLinearRegressionBuilder.build(userId)
  val hb = new HybridService(mainSubDir, 1, 1.0, 1.0)
  hb.run(cbPipeline)
}
