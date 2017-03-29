package gr.ml.analytics

import java.nio.file.Paths

import gr.ml.analytics.service.{RecommenderService, RecommenderServiceImpl}
import gr.ml.analytics.service.cf.PredictionService
import gr.ml.analytics.util.{SparkUtil, Util}
import org.apache.spark.ml.recommendation.ALS
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

  var ratingService: RecommenderService = new RecommenderServiceImpl()
  var predictionService: PredictionService = new PredictionService()

  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  val datasetsDirectory = "data"
  val smallDatasetFileName = "ml-latest-small.zip"
  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"
  val historicalRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val currentRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "current-ratings.csv").toAbsolutePath.toString

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
    val ratingsDF = predictionService.loadRatings(historicalRatingsPath)
    val userId = 1 // just an existing user

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    predictionService.writeModel(model)
    val predictions1 = predictionService.calculatePredictedIdsForUser(userId, model)
    val predictions2 = predictionService.calculatePredictedIdsForUser(userId, predictionService.readModel())
    // TODO replace with assert:
    println(predictions1.stream().limit(10).toArray.toList.equals(predictions2.stream().limit(10).toArray.toList))
  }

  "Whole prediction flow" should "run without errors" in {
    val userId: Int = 0 // new user
    ratingService.save(userId, 1, 5.0) // TODO how can we save in some other file, not to mess with a real ratings?
    ratingService.save(userId, 2, 4.0)
    ratingService.save(userId, 3, 5.0)
    ratingService.save(userId, 4, 4.0)
    ratingService.save(userId, 5, 5.0)
    ratingService.save(userId, 6, 3.0)
    ratingService.save(userId, 7, 5.0)
    ratingService.save(userId, 8, 4.0)

    predictionService.updateModel()

    // TODO method updatePredictionsForUser now returns DataFrame instead of item ids! fix the test!!
//    val predictedMovieIds = predictionService.updatePredictionsForUser(userId) // TODO uncomment
//    val predictedMovieIdsFromFile = ratingService.getTop(userId, 5) // TODO uncomment

    // TODO replace with assert
//    println(predictedMovieIds.toArray.toList.take(5).equals(predictedMovieIdsFromFile)) // TODO uncomment
  }


  "Precision for historical data" should "should be reasonable" in {

    val ratingsDF = predictionService.loadRatings(historicalRatingsPath)

    val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(training)

    val predictions = model.transform(test).filter(not(isnan($"prediction")))
    val total = predictions.count()
    val tp = predictions.filter($"rating" > 3.0 && $"prediction" > 3.0).count()
    val precision = tp.toFloat/total
    println(s"Precision = $precision") // approx. 0.5
  }
}
