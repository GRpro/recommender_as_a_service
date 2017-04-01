package gr.ml.analytics.service

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.cf.PredictionService

object HybridService extends App{

  val userId = 1
  val collaborativeReader = CSVReader.open(String.format(PredictionService.collaborativePredictionsForUserPath, userId.toString))
  val collaborativePredictions = collaborativeReader.all()
  collaborativeReader.close()
  val contentBasedReader = CSVReader.open(String.format(PredictionService.contentBasedPredictionsForUserPath, userId.toString))
  val contentBasedPredictions = contentBasedReader.all()
  contentBasedReader.close()
  val allPredictions= collaborativePredictions ++ contentBasedPredictions

  val hybridPredictions: List[List[String]]= allPredictions.filter(l=>l(0)!="userId").groupBy(l=>l(1))
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
  println(23)


}
