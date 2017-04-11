package gr.ml.analytics.service.cf

import com.github.tototoshi.csv.CSVReader
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraConnector
import gr.ml.analytics.service.Constants
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  * @param sparkSession
  * @param config
  */
class CFJob(val sparkSession: SparkSession,
            val config: Config) {

  import CFJob._

  private val itemsTable: String = config.getString("cassandra.items_table")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val keyspace: String = config.getString("cassandra.keyspace")

  private def trainModel(spark: SparkSession, ratingsDF: DataFrame): ALSModel = {
    val als = new ALS()
      .setMaxIter(5) // TODO extract into settable fields
      .setRegParam(0.01) // TODO extract into settable fields
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    als.fit(ratingsDF)
  }

//  /**
//    * Returns DataFrame with missing userId itemId pairs
//    */
//  def getNotRated(): DataFrame = {
//
//  }
//
//  def getUserMoviePairsToRate(userId: Int): DataFrame = {
//    val sparkSession = SparkUtil.sparkSession()
//    import sparkSession.implicits._
//    val itemIDsNotRateByUser = getItemIDsNotRatedByUser(userId)
//    val userMovieList: List[(Int, Int)] = itemIDsNotRateByUser.map(itemId => (userId, itemId))
//    userMovieList.toDF("userId", "itemId")
//  }
//
//  def getItemIDsNotRatedByUser(userId: Int, itemIDsDF: DataFrame): List[Int] = {
//
//    val allMovieIDs = getAllItemIDs()
//    val movieIdsRatedByUser = allRatings.filter((p:List[String])=>p(1)!="movieId" && p(0).toInt==userId)
//      .map((p:List[String]) => p(1).toInt).toSet
//    val movieIDsNotRateByUser = allMovieIDs.filter(m => !movieIdsRatedByUser.contains(m))
//    movieIDsNotRateByUser
//  }
//
//  def calculatePredictionsForUser(userId: Int, model: ALSModel): DataFrame = {
//    val sparkSession = SparkUtil.sparkSession()
//    import sparkSession.implicits._
//    val toRateDS: DataFrame = getUserMoviePairsToRate(userId)
//    import org.apache.spark.sql.functions._
//    val predictions = model.transform(toRateDS)
//      .filter(not(isnan($"prediction")))
//      .orderBy(col("prediction").desc)
//    predictions
//  }


  /**
    * Spark job entry point
    */
  def run(): Unit = {
    val spark = CassandraConnector(config).setConnectionInfo(sparkSession)

    val ratingsDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> ratingsTable, "keyspace" -> keyspace))
      .load()

    val itemsDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> itemsTable, "keyspace" -> keyspace))
      .load()

    val model = trainModel(spark, ratingsDF)

    writeModel(spark, model)

    val userIDs = ratingsDF
      .select("userId")
      .distinct()

    val allItemIDs = itemsDF
      .select("itemId")
      .distinct() // just to be on the safe size
  }
}

object CFJob extends Constants {

  private def readModel(spark: SparkSession): ALSModel = {
    val model = ALSModel.load(String.format(collaborativeModelPath, mainSubDir))
    model
  }

  private def writeModel(spark: SparkSession, model: ALSModel): ALSModel = {
    model.write.overwrite().save(String.format(collaborativeModelPath, mainSubDir))
    model
  }
}
