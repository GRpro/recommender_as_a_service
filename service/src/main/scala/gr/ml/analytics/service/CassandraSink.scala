package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

class CassandraSink(val config: Config)
                   (implicit val sparkSession: SparkSession) extends Sink {

  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  import spark.implicits._

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")
  private val cbPredictionsTable: String = config.getString("cassandra.cb_predictions_table")
  private val popularItemsTable: String = config.getString("cassandra.popular_items_table")
  private val hybridPredictionsTable: String = config.getString("cassandra.hybrid_predictions_table")
  private val recommendationsTable: String = config.getString("cassandra.recommendations_table")
  private val trainRatingsTable: String = config.getString("cassandra.train_ratings_table")
  private val testRatingsTable: String = config.getString("cassandra.test_ratings_table")

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cfPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$cbPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$popularItemsTable (itemid int PRIMARY KEY, rating float, n_ratings int)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$hybridPredictionsTable (key text PRIMARY KEY, userid int, itemid int, prediction float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$recommendationsTable (userid int PRIMARY KEY, recommended_ids text)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$trainRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float)")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$testRatingsTable (key text PRIMARY KEY, userid int, itemid int, rating float)")
  }

  override def removePredictions(predictionsTable: String): Unit = {
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"TRUNCATE $keyspace.$predictionsTable")
    }
  }

  override def storePredictions(predictions: DataFrame, predictionsTable: String): Unit = {
    predictions
      .select(col("userId").as("userid"), col("itemId").as("itemid"), col("prediction"))
      .withColumn("key", concat(col("userid"), lit(":"), col("itemid")))
      .write.mode("append")
      .cassandraFormat(predictionsTable, keyspace)
      .save()
  }

  override def storeRecommendedItemIDs(userId: Int, recommendedItemIds: List[Int]): Unit = {
    val recommendedIDsString = recommendedItemIds.toArray.mkString(":")
    CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
      session.execute(s"UPDATE $keyspace.$recommendationsTable SET recommended_ids = '$recommendedIDsString' WHERE userid = $userId")
    }
  }
}
