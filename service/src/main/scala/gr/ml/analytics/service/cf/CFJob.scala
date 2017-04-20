package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.service._
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


trait Source {
  /**
    * @return dataframe of (userId: Int, itemId: Int, rating: float) triples to train model
    */
  def all: DataFrame

  /**
    * @return Set of userIds the performed latest ratings
    */
  def getUserIdsForLastNSeconds(seconds : Long): Set[Int]

  /**
    * @return DataFrame of itemIds and userIds for rating (required by CF job)
    */
  def getUserItemsPairsToRate(userId: Int): DataFrame
}


trait Sink {
  /**
    * Store CF predictions dataframe
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: Double) triples
    */
  def storeCFPredictions(predictions: DataFrame)

  /**
    * Store CB predictions dataframe
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: Double) triples
    */
  def storeCBPredictions(predictions: DataFrame)

  /**
    * Store popular items
    *
    * @param popularItems dataframe of (itemId: Int, rating: Double, nRatings: Int) triples
    */
  def storePopularItems(popularItems: DataFrame)

  def persistPopularItemIDS()
}


class CassandraSource(val sparkSession: SparkSession, val config: Config) extends Source {

  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val itemsTable: String = config.getString("cassandra.items_table")
  private val keyspace: String = config.getString("cassandra.keyspace")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"
  private val timestampCol = "timestamp"
  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  spark.conf.set("spark.sql.crossJoin.enabled", "true")
  import spark.implicits._

  private val ratingsDS = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
    .load()
    .select(userIdCol, itemIdCol, ratingCol)

  private lazy val userIDsDS = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> itemsTable, "keyspace" -> keyspace))
    .load()
    .select(itemIdCol)

  private lazy val itemIDsDS = ratingsDS
    .select(col(itemIdCol))
    .distinct()

  /**
    * @inheritdoc
    */
  override def all: DataFrame = ratingsDS

  /**
    * @inheritdoc
    */
  override def getUserIdsForLastNSeconds(seconds: Long): Set[Int] = {
    val userIdsDF = getUserIdsForLastDF(seconds)

    val userIdsSet = userIdsDF.collect()
      .map(r => r.getInt(0))
      .toSet
    userIdsSet
  }

  def getUserIdsForLastDF(seconds: Long): DataFrame = {
    val userIdsDF = ratingsDS.filter($"timestamp" > System.currentTimeMillis()/1000 - seconds)
      .select(userIdCol).distinct()
    userIdsDF
  }

  /**
    * @inheritdoc
    */
  override def getUserItemsPairsToRate(userId: Int): DataFrame ={
    val itemIdsNotToIncludeDF = ratingsDS.filter($"userid" === userId).select("itemid") // 4 secs
    val itemIdsNotToIncludeSet = itemIdsNotToIncludeDF.collect()
      .map(r=>r.getInt(0))
      .toSet.toList
    val itemsIdsToRate = itemIDsDS.filter(!$"itemid".isin(itemIdsNotToIncludeSet: _*)) // quick
    val notRatedPairsDF = itemsIdsToRate.withColumn("userid", lit(userId))
      .select(col("itemid").as("itemId"), col("userid").as("userId"))
    notRatedPairsDF
  }
}


class CassandraSink(val sparkSession: SparkSession, val config: Config) extends Sink with Constants{
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val keyspace: String = config.getString("cassandra.keyspace")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")
  private val cbPredictionsTable: String = config.getString("cassandra.cb_predictions_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  /**
    * @inheritdoc
    */
  override def storeCFPredictions(predictions: DataFrame): Unit = {

    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cfPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    }

    predictions
      .select(col("userId").as("userid"), col("itemId").as("itemid"), col("prediction"))
      .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
      .write.mode("overwrite")
      .cassandraFormat(cfPredictionsTable, keyspace)
      .save()
  }

  /**
    * @inheritdoc
    */
  override def storeCBPredictions(predictions: DataFrame): Unit = {

    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cbPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    }

    predictions
      .select(col("userId").as("userid"), col("itemId").as("itemid"), col("prediction"))
      .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
      .write.mode("overwrite")
      .cassandraFormat(cbPredictionsTable, keyspace)
      .save()
  }

  /**
    * @inheritdoc
    */
  override def storePopularItems(predictions: DataFrame): Unit = {
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
    }

    predictions
      .write.mode("overwrite")
      .cassandraFormat(popularItemsTable, keyspace)
      .save()
  }

  /**
    * @inheritdoc
    */
  override def persistPopularItemIDS(): Unit ={
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

    val popularItemsDF = sorted.toDF("itemid", "rating", "n_ratings")
    storePopularItems(popularItemsDF)
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


/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val sparkSession: SparkSession,
            val config: Config,
            val source: Source,
            val sink: Sink,
            val params: Map[String, Any]) {

  private val ONE_DAY = 24 * 3600   // TODO Set it from redis

  private val minimumPositiveRating = 3.0

  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.getAllRatings.select("userId", "itemId", "rating")

    val rank = params.get("cf_rank").get.toString.toInt
    val regParam = params.get("cf_reg_param").get.toString.toDouble
    val als = new ALS()
      .setMaxIter(2)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    for(userId <- source.getUserIdsForLastNSeconds(ONE_DAY)){
     val notRatedPairsDF = source.getUserItemsPairsToRate(userId)
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull)
        .filter(s"prediction > $minimumPositiveRating")
        .select("userid", "itemid", "prediction")
        .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))

      sink.storeCFPredictions(predictedRatingsDS)
    }
  }
}

object CFJob extends Constants {

  def apply(sparkSession: SparkSession,
            config: Config,
            sourceOption: Option[Source],
            sinkOption: Option[Sink],
            params: Map[String, Any]): CFJob = {
    val source = sourceOption match {
      case Some(s) => s
      case None => new CassandraSource(sparkSession, config)
    }
    val sink = sinkOption match {
      case Some(s) => s
      case None => new CassandraSink(sparkSession, config)
    }
    new CFJob(sparkSession, config, source, sink, params)
  }

  private def readModel(spark: SparkSession): ALSModel = {
    val model = ALSModel.load(String.format(collaborativeModelPath, mainSubDir))
    model
  }

  private def writeModel(spark: SparkSession, model: ALSModel): ALSModel = {
    model.write.overwrite().save(String.format(collaborativeModelPath, mainSubDir))
    model
  }
}
