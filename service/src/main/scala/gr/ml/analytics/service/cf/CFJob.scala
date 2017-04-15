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
    * @return dataframe of (userId: Int, itemId: Int) pairs to predict ratings for
    */
  def toRate: DataFrame
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
  private val keyspace: String = config.getString("cassandra.keyspace")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  spark.conf.set("spark.sql.crossJoin.enabled", "true")

  private lazy val ratingsDS = spark
    .read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> ratingsTable, "keyspace" -> keyspace))
    .load()
    .select(userIdCol, itemIdCol, ratingCol)

  private lazy val userIDsDS = ratingsDS
    .select(col(userIdCol))
    .distinct()

  private lazy val itemIDsDS = ratingsDS
    .select(col(itemIdCol))
    .distinct()

  // TODO replace this cross join operation
  private lazy val notRatedPairsDS = itemIDsDS
    .join(userIDsDS)
    .except(ratingsDS.select(col(userIdCol), col(itemIdCol)))

  /**
    * @inheritdoc
    */
  override def all: DataFrame = ratingsDS

  /**
    * @inheritdoc
    */
  override def toRate: DataFrame = notRatedPairsDS
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
            val config: Config,
            val source: Source,
            val sink: Sink) {

  import CFJob._

  private val minimumPositiveRating = 3.0

  /**
    * Spark job entry point
    */
  def run(): Unit = {

    val allRatingsDF = source.all.select("userId", "itemId", "rating")

    val als = new ALS()
      .setMaxIter(2) // TODO extract into settable fields
      .setRegParam(0.1) // TODO extract into settable fields
      .setUserCol("userId")
      .setItemCol("itemId")
      .setRatingCol("rating")

    val model = als.fit(allRatingsDF)

    writeModel(sparkSession, model)

    val notRatedPairsDF = source.toRate.select("userId", "itemId")

    val predictedRatingsDS = model.transform(notRatedPairsDF)
      .filter(col("prediction").isNotNull)
      .filter(s"prediction > $minimumPositiveRating")
      .select("userId", "itemId", "prediction")

    // print 1000 to console
    predictedRatingsDS.show(1000)

    sink.store(predictedRatingsDS)
  }
}

object CFJob extends Constants {

  def apply(sparkSession: SparkSession,
            config: Config,
            sourceOption: Option[Source],
            sinkOption: Option[Sink]): CFJob = {
    val source = sourceOption match {
      case Some(s) => s
      case None => new CassandraSource(sparkSession, config)
    }
    val sink = sinkOption match {
      case Some(s) => s
      case None => new CassandraSink(sparkSession, config)
    }
    new CFJob(sparkSession, config, source, sink)
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
