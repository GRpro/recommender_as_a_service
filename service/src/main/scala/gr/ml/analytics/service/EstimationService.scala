package gr.ml.analytics.service

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.service.contentbased.{DecisionTreeRegressionBuilder, LinearRegressionWithElasticNetBuilder, RandomForestEstimatorBuilder}
import gr.ml.analytics.util_old.{DataUtil, Util}
import org.apache.spark.ml.Pipeline

object EstimationService extends App with Constants{
  val trainFraction = 0.7
  val upperFraction = 0.4
  val lowerFraction = 0.4
  val subRootDir = "precision"

  Util.loadAndUnzip(subRootDir)
  divideRatingsIntoTrainAndTest()
  val numberOfTrainRatings = getNumberOfTrainRatings()
  val hb = new HybridService(subRootDir, numberOfTrainRatings, 1.0, 1.0)
  hb.prepareNecessaryFiles()

  var bestAccuracy = 0.0
  var bestParams = (0.0, 0.0)
  var bestCBPipeline: Pipeline = null

  val pipelines:List[Pipeline] = List(
    LinearRegressionWithElasticNetBuilder.build(subRootDir),
    RandomForestEstimatorBuilder.build(subRootDir),
    DecisionTreeRegressionBuilder.build(subRootDir))

  pipelines.foreach(pipeline => {
    hb.runOneCycle(pipeline)
    (0.0 to 1.0 by 0.01).foreach(i=>{
      hb.combinePredictionsForLastUsers(i, 1-i)
      val accuracy = estimateAccuracy(upperFraction, lowerFraction)
      println("LinearRegressionWithElasticNetBuilder:: Weights: " + i + ", " + (1-i) + " => Accuracy: " + accuracy)
      if(accuracy > bestAccuracy){
        bestAccuracy = accuracy
        bestParams = (i, 1-i)
        bestCBPipeline = pipeline
      }
    })
  })

  println("Best Accuracy is " + bestAccuracy + " for pipeline: " + bestCBPipeline.getStages +  " and params " + bestParams)

  def divideRatingsIntoTrainAndTest(): Unit ={
    val ratingsReader = CSVReader.open(String.format(ratingsPathSmall, subRootDir)) // TODO replace with all ratings
    val allRatings = ratingsReader.all().filter(l=>l(0)!="userId")
    ratingsReader.close()
    val trainHeaderWriter = CSVWriter.open(String.format(ratingsPath, subRootDir), append = false)
    trainHeaderWriter.writeRow(List("userId", "itemId", "rating", "timestamp"))
    trainHeaderWriter.close()
    val testHeaderWriter = CSVWriter.open(String.format(testRatingsPath, subRootDir), append = false)
    testHeaderWriter.writeRow(List("userId", "itemId", "rating", "timestamp"))
    testHeaderWriter.close()

    val trainWriter = CSVWriter.open(String.format(ratingsPath, subRootDir), append = true)
    val testWriter = CSVWriter.open(String.format(testRatingsPath, subRootDir), append = true)
    allRatings.groupBy(l=>l(0)).foreach(l=>{
      val trainTestTuple = l._2.splitAt((l._2.size * trainFraction).toInt)
      trainWriter.writeAll(trainTestTuple._1)
      testWriter.writeAll(trainTestTuple._2)
    })
    trainWriter.close()
    testWriter.close()
  }

  def getNumberOfTrainRatings(): Int ={
    val ratingsReader = CSVReader.open(String.format(ratingsPath, subRootDir))
    val trainRatingsNumber = ratingsReader.all().size
    ratingsReader.close()
    trainRatingsNumber
  }

  def estimateAccuracy(upperFraction: Double, lowerFraction: Double): Double ={
    val testRatingsReader = CSVReader.open(String.format(testRatingsPath, subRootDir))
    val testRatings = testRatingsReader.all()
    testRatingsReader.close()

    var allFinalPredictions: List[List[String]] = List()
    val userIds = new DataUtil(subRootDir).getUserIdsFromLastNRatings(getNumberOfTrainRatings())
    for(userId <- userIds){
      val finalPredictionsReader = CSVReader.open(String.format(finalPredictionsForUserPath, subRootDir, userId.toString))
      allFinalPredictions ++= finalPredictionsReader.all()
      finalPredictionsReader.close()
    }

    val testRatingsLabeled = labelAsPositiveOrNegative(testRatings, upperFraction, lowerFraction)
    val finalPredictionsLabeled = labelAsPositiveOrNegative(allFinalPredictions, upperFraction, lowerFraction)
    val bothTestRatingsAndPredictionsLabeled = testRatingsLabeled ++ finalPredictionsLabeled

    val testPredictionPairs = bothTestRatingsAndPredictionsLabeled.groupBy(l=>(l(0),l(1))).filter(t=>t._2.size == 2)

    val correctlyPredicted = testPredictionPairs.filter(t=>t._2(0)(3)==t._2(1)(3)).size

    correctlyPredicted.toDouble/testPredictionPairs.size
  }

  def labelAsPositiveOrNegative(ratings: List[List[String]], upperFraction: Double, lowerFraction: Double): List[List[_]] ={
    // userId, itemId, label
    val userIdInd = ratings(0).indexOf("userId")
    val itemIdInd = ratings(0).indexOf("itemId")
    val ratingInd = ratings(0).indexOf("rating")

    val justRatings = ratings.filter(l=>l(userIdInd)!="userId").map(l=>l(ratingInd).toDouble)
    val ratingRange = justRatings.max - justRatings.min
    val upperLimit = justRatings.min + (1.0-upperFraction)*ratingRange
    val lowerLimit = justRatings.min + lowerFraction*ratingRange

    val labeledRatings = ratings.filter(l=>l(userIdInd)!="userId").filter(l=>l(ratingInd).toDouble >= upperLimit || l(ratingInd).toDouble < lowerLimit)
        .map(l=>List(l(userIdInd), l(itemIdInd), l(ratingInd), if(l(ratingInd).toDouble >= upperLimit) 1 else 0))

    labeledRatings
  }

}
