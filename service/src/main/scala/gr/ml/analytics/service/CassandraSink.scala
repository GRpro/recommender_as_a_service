package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

class CassandraSink(val sparkSession: SparkSession, val config: Config) extends Sink {

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)
  import spark.implicits._

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")
  private val cbPredictionsTable: String = config.getString("cassandra.cb_predictions_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cfPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cbPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
  }

  def storePredictions(predictions: DataFrame, predictionsTable: String): Unit = {
    predictions
      .select(col("userId").as("userid"), col("itemId").as("itemid"), col("prediction"))
      .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
      .write.mode("overwrite")
      .cassandraFormat(predictionsTable, keyspace)
      .save()
  }

  /**
    * @inheritdoc
    */
  override def storeCBPredictions(predictions: DataFrame): Unit = {
    storePredictions(predictions, cbPredictionsTable)
  }

  /**
    * @inheritdoc
    */
  override def storeCFPredictions(predictions: DataFrame): Unit = {
    storePredictions(predictions, cfPredictionsTable)
  }

  /**
    * @inheritdoc
    */
  override def persistPopularItems(): Unit ={
    val ratingsDS = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
      .load()
      .select(userIdCol, itemIdCol, ratingCol)

    val allRatings = ratingsDS.collect().map(r => List(r.getInt(0), r.getInt(1), r.getDouble(2)))

    // TODO we can use it as a general method both for files and cassandra
    val mostPopular = allRatings.filter(l=>l(1)!="itemId").groupBy(l=>l(1))
      .map(t=>(t._1, t._2, t._2.size))
      .map(t=>(t._1, t._2.reduce((l1,l2)=>List(l1(0), l1(1), (l1(2) + l2(2)))), t._3))
      .map(t=>(t._1,t._2(2).toString.toDouble / t._3.toDouble, t._3)) // calculate average rating
      .toList.sortWith((tl,tr) => tl._3 > tr._3) // sorting by number of ratings
      .take(allRatings.size/10) // take first 1/10 of items sorted by number of ratings

    val maxRating: Double = mostPopular.sortWith((tl,tr)=>tl._2.toInt > tr._2.toInt).head._2
    val maxNumberOfRatings: Int = mostPopular.sortWith((tl,tr)=>tl._3 > tr._3).head._3

    val sorted = mostPopular.sortWith(sortByRatingAndPopularity(maxRating,maxNumberOfRatings))
      .map(t=>(t._1, t._2, t._3))

    val popularItemsDF: DataFrame = sorted.toDF("itemid", "rating", "n_ratings")
    popularItemsDF
      .write.mode("overwrite")
      .cassandraFormat(popularItemsTable, keyspace)
      .save()
  }

  private def sortByRatingAndPopularity(maxRating:Double, maxRatingsNumber:Int) ={
    // Empirical coefficient to make popular high rated movies go first
    // (suppressing unpopular but high-rated movies by small number of individuals)
    // Math.PI is just for more "scientific" look ;-)
    val coef = Math.PI * Math.sqrt(maxRatingsNumber.toDouble)/Math.sqrt(maxRating)
    (tl:(Double,Double,Int), tr:(Double,Double,Int)) =>
      Math.sqrt(tl._3) + coef * Math.sqrt(tl._2) > Math.sqrt(tr._3) + coef * Math.sqrt(tr._2)
  }
}
