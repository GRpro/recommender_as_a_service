package gr.ml.analytics

import java.nio.file.Paths

import gr.ml.analytics.model.Util
import gr.ml.analytics.service.PredictionService
import org.slf4j.LoggerFactory

object PredictionServiceRunner extends App with Constants {

  val predictionService: PredictionService = new PredictionService()
  val progressLogger = LoggerFactory.getLogger("progressLogger")

  //TODO remove this and oll related stuff. This is temporary fix for Windows
  if (System.getProperty("os.name").contains("Windows")) {
    val HADOOP_BIN_PATH = getClass.getClassLoader.getResource("").getPath
    System.setProperty("hadoop.home.dir", HADOOP_BIN_PATH)
  }

  // download Datasets

  Util.loadResource(smallDatasetUrl,
    Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
  Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
    Paths.get(datasetsDirectory).toAbsolutePath)

  def tryAndLog(method: => Unit, message: String): Unit ={
    progressLogger.info(message)
    try {
      method
    } catch {
      case _: Exception => progressLogger.error("Error during " + message)
    }
  }

  /*

  Periodically run batch job which updates model

   */
  while(true) {
    Thread.sleep(1000)
    tryAndLog(predictionService.updateModel, "Updating model")
    tryAndLog(predictionService.updatePredictionsForUser(0), "Updating predictions for User " + 0) // TODO add method getUserIdsForPrediction (return all unique user ids from current-ratings.csv)
  }

}
