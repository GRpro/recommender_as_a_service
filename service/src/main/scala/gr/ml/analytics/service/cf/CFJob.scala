package gr.ml.analytics.service.cf

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import gr.ml.analytics.service.Constants
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.cassandra._
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
  def getUserIdsForLast(seconds : Long): Set[Int]

  /**
    * @return DataFrame of itemIds and userIds for rating (required by CF job)
    */
  def getUserItemsPairsToRate(userId: Int): DataFrame
}


trait Sink {
  /**
    * Store predictions dataframe
    *
    * @param predictions dataframe of (userId: Int, itemId: Int, prediction: float) triples
    */
  def store(predictions: DataFrame)
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
  override def getUserIdsForLast(seconds: Long): Set[Int] = {
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


class CassandraSink(val sparkSession: SparkSession, val config: Config) extends Sink {

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  /**
    * @inheritdoc
    */
  override def store(predictions: DataFrame): Unit = {

    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
      session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cfPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    }

    val normalized = predictions
      .select("userId", "itemId", "prediction")
      .toDF("userid", "itemid", "prediction").withColumn("key", concat(col("userid"), lit(":"), col("itemid")))

    normalized.show(1000)
    normalized
      .write.mode("overwrite")
      .cassandraFormat(cfPredictionsTable, keyspace)
      .save()
  }
}


/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  */
class CFJob(val sparkSession: SparkSession,
            val source: Source,
            val sink: Sink,
            val params: Map[String, Any]) {

  private val ONE_DAY = 24 * 3600

  private val minimumPositiveRating = 3.0

  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.all.select("userId", "itemId", "rating")

    val rank = params.get("rank").get.toString.toInt
    val regParam = params.get("reg_param").get.toString.toDouble
    val als = new ALS()
      .setMaxIter(2)
      .setRegParam(regParam)
      .setRank(rank)
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    for(userId <- source.getUserIdsForLast(ONE_DAY)){
     val notRatedPairsDF = source.getUserItemsPairsToRate(userId)
      val predictedRatingsDS = model.transform(notRatedPairsDF)
        .filter(col("prediction").isNotNull)
        .filter(s"prediction > $minimumPositiveRating")
        .select("userId", "itemId", "prediction")

      sink.store(predictedRatingsDS)
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
    new CFJob(sparkSession, source, sink, params)
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
