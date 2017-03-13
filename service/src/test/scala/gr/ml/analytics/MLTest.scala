package gr.ml.analytics

import java.io._
import java.nio.file.Paths

import gr.ml.analytics.model._
import gr.ml.analytics.service.RatingService
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{isnan, not, udf}
import org.scalatest._

/**
  * @author hrozhkov
  */
class MLTest extends FlatSpec with BeforeAndAfterAllConfigMap{

  lazy val sparkSession = SparkUtil.sparkSession()
  import sparkSession.implicits._

  var ratingService: RatingService = new RatingService()

  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toLong: UserDefinedFunction = udf[Long, String](_.toLong)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  val datasetsDirectory = "data"
  val smallDatasetFileName = "ml-latest-small.zip"
  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"
  val historicalRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val currentRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "current-ratings.csv").toAbsolutePath.toString
  val bothRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small").toAbsolutePath.toString + File.separator + "*ratings.csv"
  val predictionsPath = Paths.get(datasetsDirectory, "ml-latest-small", "predictions.csv").toAbsolutePath.toString
  val modelPath = Paths.get(datasetsDirectory, "model").toAbsolutePath.toString

  override def beforeAll(configMap: ConfigMap): Unit = {
    Util.loadResource(smallDatasetUrl,
      Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
    Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
      Paths.get(datasetsDirectory).toAbsolutePath)

    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(currentRatingsPath)
    writer.writeRow(List("userId", "movieId", "rating", "timestamp"))
  }

  "DataFrame" should "be persisted without error" in {
    val df: DataFrame = List(1,2,3).toDF("first")
    val testDFPath = Paths.get(datasetsDirectory, "test", "df.out").toAbsolutePath.toString
    df.write.mode(SaveMode.Overwrite).save(testDFPath)
    val df2 = sparkSession.read.load(testDFPath)
    println(df.collectAsList() == df2.collectAsList()); // TODO replace with assert
  }

  "Persisted ALS model" should "give the same result" in {
    val ratingsDF = ratingService.loadRatings(historicalRatingsPath)
    val userId = 1 // just an existing user

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    ratingService.writeModel(model)
    val predictions1 = ratingService.calculatePredictedIdsForUser(userId, model)
    val predictions2 = ratingService.calculatePredictedIdsForUser(userId, ratingService.readModel())
    // TODO replace with assert:
    println(predictions1.stream().limit(10).toArray.toList.equals(predictions2.stream().limit(10).toArray.toList))
  }

  "Whole prediction flow" should "run without errors" in {
    val userId: Int = 0 // new user
    ratingService.persistRating(userId, 1, 5.0)
    ratingService.persistRating(userId, 2, 4.0)
    ratingService.persistRating(userId, 3, 5.0)
    ratingService.persistRating(userId, 4, 4.0)
    ratingService.persistRating(userId, 5, 5.0)
    ratingService.persistRating(userId, 6, 3.0)
    ratingService.persistRating(userId, 7, 5.0)
    ratingService.persistRating(userId, 8, 4.0)

    ratingService.updateModel()

    val predictedMovieIds = ratingService.updatePredictionsForUser(userId)
    val predictedMovieIdsFromFile = ratingService.loadPredictions(userId, 5)

    // TODO replace with assert
    println(predictedMovieIds.toArray.toList.take(5).equals(predictedMovieIdsFromFile))
  }


  "Precision for historical data" should "should be reasonable" in {

    val ratingsDF = ratingService.loadRatings(historicalRatingsPath)

    val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

    val predictions = model.transform(test).filter(not(isnan($"prediction")))
    val total = predictions.count() // TODO VERY time-consuming, can we improve?
    val tp = predictions.filter($"rating" > 3.0 && $"prediction" > 3.0).count() // TODO VERY time-consuming, can we improve?
    val precision = tp.toFloat/total
    println(s"Precision = $precision") // approx. 0.5
  }
}
