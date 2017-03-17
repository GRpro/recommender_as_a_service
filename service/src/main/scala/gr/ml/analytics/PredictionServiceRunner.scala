package gr.ml.analytics

import gr.ml.analytics.service.PredictionService
import org.slf4j.LoggerFactory

object PredictionServiceRunner{
  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")
  def main(args: Array[String]): Unit = {
    while (true){
      tryAndLog(predictionService.updateModel, "Updating model")
      tryAndLog(predictionService.updatePredictionsForUser(0), "Updating predictions for User " + 0) // TODO add method getUserIdsForPrediction (return all unique user ids from current-ratings.csv)
    }
  }

  def tryAndLog(method: => Unit, message: String): Unit ={
    progressLogger.info(message)
    try{
      method
    }
    catch{
      case e: Exception => progressLogger.error("Error during " + message)
    }
  }
}
