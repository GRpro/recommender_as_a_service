package gr.ml.analytics.service

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.service.contentbased.{CBPredictionService, LinearRegressionWithElasticNetBuilder}
import gr.ml.analytics.util.{CSVtoSVMConverter, GenresFeatureEngineering, Util}

object HybridService extends App{
  val collaborativeWeight = 1.0
  val contentBasedWeight = 1.0

  Util.windowsWorkAround()
//  Util.loadAndUnzip()

  // TODO add checking if file exists.
  //  GenresFeatureEngineering.createAllRatingsWithFeaturesFile() // TODO uncomment!
  CSVtoSVMConverter.createSVMRatingFilesForCurrentUsers()

  val userIds = new PredictionService().getUserIdsForPrediction()

  while(true){
    Thread.sleep(5000)
    Util.tryAndLog(new PredictionService().updateModel(), "Collaborative:: Updating model")
    for(userId <- userIds){
      val pipeline = LinearRegressionWithElasticNetBuilder.build(userId)
      Util.tryAndLog(CBPredictionService.updateModelForUser(pipeline, userId), "Content-based:: Updating model for user " + userId)
      Util.tryAndLog(new PredictionService().updatePredictionsForUser(userId), "Collaborative:: Updating predictions for User " + userId)
      Util.tryAndLog(CBPredictionService.updatePredictionsForUser(userId), "Content-based:: Updating predictions for User " + userId)
      Util.tryAndLog(combinePredictionsForUser(userId), "Hybrid:: Combining CF and CB predictions for user " + userId)
    }
  }

  def multiplyByWeight(predictions: List[List[String]], weight: Double): List[List[String]] ={
    predictions.map(l=>List(l(0), l(1), (l(2).toDouble * weight).toString))
  }

  def combinePredictionsForUser(userId: Int): Unit ={
    new File(PredictionService.finalPredictionsDirectoryPath).mkdirs()
    val collaborativeReader = CSVReader.open(String.format(PredictionService.collaborativePredictionsForUserPath, userId.toString))
    val collaborativePredictions = collaborativeReader.all().filter(l=>l(0)!="userId")
    collaborativeReader.close()
    val contentBasedReader = CSVReader.open(String.format(PredictionService.contentBasedPredictionsForUserPath, userId.toString))
    val contentBasedPredictions = contentBasedReader.all().filter(l=>l(0)!="userId")
    contentBasedReader.close()

    val weightedCollaborativePredictions = multiplyByWeight(collaborativePredictions, collaborativeWeight)
    val weightedContentBasedPredictions = multiplyByWeight(contentBasedPredictions, contentBasedWeight)

    val allPredictions= weightedCollaborativePredictions ++ weightedContentBasedPredictions

    val hybridPredictions: List[List[String]]= allPredictions.groupBy(l=>l(1))
      .map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2).toDouble+l2(2).toDouble).toString))))
      .map(t=>t._2).toList.sortWith((l,r) => l(2).toDouble > r(2).toDouble)

    val finalPredictionsHeaderWriter = CSVWriter.open(String.format(PredictionService.finalPredictionsForUserPath, userId.toString), append = false)
    finalPredictionsHeaderWriter.writeRow(List("userId", "movieId", "prediction"))
    finalPredictionsHeaderWriter.close()
    val finalPredictionsWriter = CSVWriter.open(String.format(PredictionService.finalPredictionsForUserPath, userId.toString), append = true)
    finalPredictionsWriter.writeAll(hybridPredictions)
    finalPredictionsWriter.close()

    val finalPredictedIDs = hybridPredictions.map(l=>l(1).toInt)
    new PredictionService().persistPredictedIdsForUser(userId, finalPredictedIDs)
  }
}
