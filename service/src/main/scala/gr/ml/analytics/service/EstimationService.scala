package gr.ml.analytics.service

import java.io.File

import com.github.tototoshi.csv.{CSVReader, CSVWriter}

object EstimationService extends App with Constants{
  val trainFraction = 0.7
  divideRatingsIntoTrainAndTest()



  def divideRatingsIntoTrainAndTest(): Unit ={
    val ratingsReader = CSVReader.open(ratingsPathSmall) // TODO replace with all ratings
    val allRatings = ratingsReader.all()
    ratingsReader.close()
    new File(estimationDirectoryPath).mkdirs()
    val trainHeaderWriter = CSVWriter.open(trainRatingsPathSmall, append = false)
    trainHeaderWriter.writeRow(List("userId", "itemId", "rating", "timestamp"))
    trainHeaderWriter.close()
    val testHeaderWriter = CSVWriter.open(testRatingsPathSmall, append = false)
    testHeaderWriter.writeRow(List("userId", "itemId", "rating", "timestamp"))
    testHeaderWriter.close()

    val trainWriter = CSVWriter.open(trainRatingsPathSmall, append = true)
    val testWriter = CSVWriter.open(testRatingsPathSmall, append = true)
    val output = allRatings.filter(l=>l(0)!="userId")
      .groupBy(l=>l(0)).foreach(l=>{
      val trainTestTuple = l._2.splitAt((l._2.size * trainFraction).toInt)
      trainWriter.writeAll(trainTestTuple._1)
      testWriter.writeAll(trainTestTuple._2)
    })
    trainWriter.close()
    testWriter.close()
  }
}
