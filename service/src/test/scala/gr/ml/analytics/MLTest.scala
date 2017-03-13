package gr.ml.analytics

import java.io._
import java.nio.file.Paths

import com.github.tototoshi.csv.CSVReader
import gr.ml.analytics.model._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{isnan, not, udf}
import org.scalatest._

/**
  * @author hrozhkov
  */
class MLTest extends FlatSpec with BeforeAndAfterAllConfigMap{

  lazy val sparkSession = SparkUtil.sparkSession()
  import sparkSession.implicits._

  var dataStore: DataStore = null
  var predictionModelCreator: PredictionModelCreator = null
  var predictionModel: PredictionModel = null

  val toInt: UserDefinedFunction = udf[Int, String](_.toInt)
  val toLong: UserDefinedFunction = udf[Long, String](_.toLong)
  val toDouble: UserDefinedFunction = udf[Double, String](_.toDouble)

  val datasetsDirectory = "datasets"
  val smallDatasetFileName = "ml-latest-small.zip"
  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"
  val historicalRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val currentRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small", "current-ratings.csv").toAbsolutePath.toString
  val bothRatingsPath = Paths.get(datasetsDirectory, "ml-latest-small").toAbsolutePath.toString + File.separator + "*ratings.csv"

  // TODO add automatical current-ratings file creation!

  override def beforeAll(configMap: ConfigMap): Unit = {
    Util.loadResource(smallDatasetUrl,
      Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath)
    Util.unzip(Paths.get(datasetsDirectory, smallDatasetFileName).toAbsolutePath,
      Paths.get(datasetsDirectory).toAbsolutePath)

    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(currentRatingsPath)
    writer.writeRow(List("userId", "movieId", "rating", "timestamp"))
  }

  "Persisted ALS model" should "give the same result" in {
    val ratingsDF = loadRatings(historicalRatingsPath)
    val userId = 1 // just an existing user

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    writeModel(model)
    val predictions1 = getPredictionsForUser(userId, model, 5)
    val predictions2 = getPredictionsForUser(userId, readModel(), 5)
    // TODO replace with assert:
    println(predictions1.collectAsList().equals(predictions2.collectAsList()))
  }

  "Whole prediction flow" should "run without errors" in {
    val userId: Int = 0 // new user
    persistUserRating(userId, 1, 5.0)
    persistUserRating(userId, 2, 4.0)
    persistUserRating(userId, 3, 5.0)
    persistUserRating(userId, 4, 4.0)
    persistUserRating(userId, 5, 5.0)
    persistUserRating(userId, 6, 3.0)
    persistUserRating(userId, 7, 5.0)
    persistUserRating(userId, 8, 4.0)

    updateModel()

    val predictions = getPredictionsForUser(userId, readModel(), 5)
    // TODO cache/save predictions

    println("Predictions:")
    predictions.show()
  }


  "Precision for historical data" should "should be reasonable" in {

    val ratingsDF = loadRatings(historicalRatingsPath)

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

  def loadRatings(path: String): DataFrame ={
    val ratingsDF = {
      val ratingsStringDF = sparkSession.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("mode", "DROPMALFORMED")
        .load(path) // TODO can we provide several different locations?
        .select("userId", "movieId", "rating", "timestamp")

      ratingsStringDF
        .withColumn("userId", toInt(ratingsStringDF("userId")))
        .withColumn("movieId", toInt(ratingsStringDF("movieId")))
        .withColumn("rating", toDouble(ratingsStringDF("rating")))
    }
    ratingsDF
  }

  def getPredictionsForUser(userId: Int, model: ALSModel, number: Int): DataFrame ={
    val toRateDS: DataFrame = getMoviesNotRatedByUser(userId)
    import org.apache.spark.sql.functions._
    val predictions = model.transform(toRateDS).orderBy(col("prediction").desc).limit(number);
    predictions
  }

  def updateModel(): Unit ={
    val ratingsDF = loadRatings(bothRatingsPath)

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")

    val model = als.fit(ratingsDF)
    writeModel(model)
  }

  def readModel(): ALSModel ={
    ALSModel.load("model.dat")
  }

  def writeModel(model: ALSModel): Unit ={
    model.write.overwrite().save("model.dat")
  }

  def persistUserRating(userId: Int, movieId: Int, rating: Double): Unit ={
    import com.github.tototoshi.csv._
    val writer = CSVWriter.open(currentRatingsPath, append = true)
    writer.writeRow(List(userId.toString, movieId.toString,rating.toString,(System.currentTimeMillis/1000).toString))
  }

  def getMoviesNotRatedByUser(userId: Int): DataFrame ={
    val reader = CSVReader.open(historicalRatingsPath) // TODO add support of several files, not just historical data
    // TODO could we do it more effective that reading all the file at once? Would be still OK for larger files?
    val movieIds = reader.all().filter((p:List[String])=>p(0)!=userId && p(1)!="movieId").map((p:List[String]) => p(1).toInt).toSet
    val userMovieList: List[(Int, Int)] = movieIds.map(movieId => (userId, movieId)).toList
    userMovieList.toDF("userId", "movieId")
  }
}
