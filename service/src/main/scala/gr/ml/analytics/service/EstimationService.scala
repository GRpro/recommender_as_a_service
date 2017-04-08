package gr.ml.analytics.service

import com.github.tototoshi.csv.{CSVReader, CSVWriter}
import gr.ml.analytics.util.Util

object EstimationService extends App with Constants{
  val trainFraction = 0.7
  val subRootDir = "estimation"
  Util.loadAndUnzip(subRootDir)
  divideRatingsIntoTrainAndTest()
  val numberOfTrainRatings = getNumberOfTrainRatings()
  val hb = new HybridService(subRootDir, numberOfTrainRatings)
  hb.prepareNecessaryFiles()
  hb.runOneCycle()

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
}
