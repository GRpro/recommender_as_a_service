package gr.ml.analytics.service

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.service.cf.{CFJob, CFPredictionService}
import gr.ml.analytics.service.contentbased.{CBPredictionService, RandomForestEstimatorBuilder}
import gr.ml.analytics.util._
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class HybridService(subRootDir: String, sparkSession: SparkSession, config: Config, source: Source, sink: Sink, paramsStorage: ParamsStorage) extends Constants{

  val cfPredictionService = new CFPredictionService(subRootDir)
  val cbPredictionService = new CBPredictionService(subRootDir)
  val csv2svmConverter = new CSVtoSVMConverter(subRootDir)

  def run(cbPipeline: Pipeline): Unit ={
    prepareNecessaryFiles() // TODO remove it when we have cassandra implementation of CB
    sink.persistPopularItems()

    while(true){
      Thread.sleep(5000) // can be increased for production

      CFJob(sparkSession, config, None, None, paramsStorage.getParams()).run() // TODO add logging somehow

      // CBJob:
      val lastNSeconds = paramsStorage.getParams().get("hb_last_n_seconds").get.toString.toLong
      val userIds = source.getUserIdsForLastNSeconds(lastNSeconds)
      for(userId <- userIds){
        Util.tryAndLog(cbPredictionService.updateModelForUser(cbPipeline, userId), subRootDir + " :: Content-based:: Updating model for user " + userId)
        Util.tryAndLog(cbPredictionService.updatePredictionsForUser(userId), subRootDir + " :: Content-based:: Updating predictions for User " + userId)
        LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: ##################### END OF ITERATION ##########################")
      }

      val collaborativeWeight = paramsStorage.getParams().get("hb_collaborative_weight").get.toString.toLong
      combinePredictionsForLastUsers(collaborativeWeight)
    }

  }

  def prepareNecessaryFiles(): Unit ={
    val startTime = System.currentTimeMillis()
    if(!(new File(String.format(moviesWithFeaturesPath, subRootDir)).exists()))
      new GenresFeatureEngineering(subRootDir).createAllMoviesWithFeaturesFile()
    val lastNSeconds = paramsStorage.getParams().get("hb_last_n_seconds").get.toString.toLong
    val userIds = source.getUserIdsForLastNSeconds(lastNSeconds)
    userIds.foreach(csv2svmConverter.createSVMRatingsFileForUser)
    if(!(new File(allMoviesSVMPath).exists()))
      csv2svmConverter.createSVMFileForAllItems()
    val finishTime = System.currentTimeMillis()

    LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: Startup time took: " + (finishTime - startTime) + " millis.")
  }

//  def runOneCycle(cbPipeline: Pipeline): Unit ={
//    Util.tryAndLog(cfPredictionService.updateModel(), subRootDir + " :: Collaborative:: Updating model")
//    val lastNSeconds = paramsStorage.getParams().get("hb_last_n_seconds").get.toString.toLong
//    val userIds = source.getUserIdsForLastNSeconds(lastNSeconds)
//    for(userId <- userIds){
//      Util.tryAndLog(cbPredictionService.updateModelForUser(cbPipeline, userId), subRootDir + " :: Content-based:: Updating model for user " + userId)
//      Util.tryAndLog(cfPredictionService.updatePredictionsForUser(userId), subRootDir + " :: Collaborative:: Updating predictions for User " + userId)
//      Util.tryAndLog(cbPredictionService.updatePredictionsForUser(userId), subRootDir + " :: Content-based:: Updating predictions for User " + userId)
//      LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: ##################### END OF ITERATION ##########################")
//    }
//  }

  def multiplyByWeight(predictions: List[List[String]], weight: Double): List[List[String]] ={
    predictions.map(l=>List(l(0), l(1), (l(2).toDouble * weight).toString))
  }

  def combinePredictionsForLastUsers(collaborativeWeight: Double): Unit ={
    val lastNSeconds = paramsStorage.getParams().get("hb_last_n_seconds").get.toString.toLong
    val userIds = source.getUserIdsForLastNSeconds(lastNSeconds)
    for(userId <- userIds){
//      Util.tryAndLog(combinePredictionsForUser(userId, collaborativeWeight, contentBasedWeight), subRootDir + " :: Hybrid:: Combining CF and CB predictions for user " + userId)
      combinePredictionsForUser(userId, collaborativeWeight)
    }
  }

  def combinePredictionsForUser(userId: Int, collaborativeWeight: Double): Unit ={
    new File(String.format(finalPredictionsDirectoryPath, subRootDir)).mkdirs()
    val collaborativeReader = CSVReader.open(String.format(collaborativePredictionsForUserPath, subRootDir, userId.toString))
    val collaborativePredictions = collaborativeReader.all().filter(l=>l(0)!="userId")
    collaborativeReader.close()
    val contentBasedReader = CSVReader.open(String.format(contentBasedPredictionsForUserPath, subRootDir, userId.toString))
    val contentBasedPredictions = contentBasedReader.all().filter(l=>l(0)!="userId")
    contentBasedReader.close()

    val weightedCollaborativePredictions = multiplyByWeight(collaborativePredictions, collaborativeWeight)
    val weightedContentBasedPredictions = multiplyByWeight(contentBasedPredictions, 1-collaborativeWeight)

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
//    cfPredictionService.persistPredictedIdsForUser(userId, finalPredictedIDs) // TODO USE CASSANDRA!!!
  }
}

object HybridServiceRunner extends App with Constants{
  Util.windowsWorkAround()
  Util.loadAndUnzip(mainSubDir) // TODO new ratings will be rewritten!!
  //      val cbPipeline = LinearRegressionWithElasticNetBuilder.build(userId)
  val cbPipeline = RandomForestEstimatorBuilder.build(mainSubDir)
  //      val cbPipeline = GeneralizedLinearRegressionBuilder.build(userId)
  val sparkSession = SparkUtil.sparkSession()
  val config = ConfigFactory.load("application.conf")
  val source = new CassandraSource(sparkSession, config)
  val sink = new CassandraSink(sparkSession, config)
  val paramsStorage: ParamsStorage = new RedisParamsStorage
  val hb = new HybridService(mainSubDir, sparkSession, config, source, sink, paramsStorage)
  hb.run(cbPipeline)
}
