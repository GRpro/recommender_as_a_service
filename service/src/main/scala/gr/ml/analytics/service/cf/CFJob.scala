package gr.ml.analytics.service.cf

import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraConnector
import gr.ml.analytics.service.Constants
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._

/**
  * Calculates ratings for missing user-item pairs using ALS collaborative filtering algorithm
  * @param sparkSession
  * @param config
  */
class CFJob(val sparkSession: SparkSession,
            val config: Config) {

  import CFJob._

  private val cfPredictionsTable: String = config.getString("cassandra.cf_predictions_table")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val keyspace: String = config.getString("cassandra.keyspace")

  private val minimumPositiveRating = 3.0

  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"

  private def trainModel(spark: SparkSession, ratingsDF: DataFrame): ALSModel = {
    val als = new ALS()
      .setMaxIter(5) // TODO extract into settable fields
      .setRegParam(0.01) // TODO extract into settable fields
      .setUserCol(userIdCol)
      .setItemCol(itemIdCol)
      .setRatingCol(ratingCol)

    als.fit(ratingsDF)
  }

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

    ratingsDF.printSchema()

    val model = trainModel(spark, ratingsDF)

    writeModel(spark, model)

    val userIDs = ratingsDF
      .select(userIdCol)
      .distinct()

    val itemIDs = ratingsDF
      .select(itemIdCol)
      .distinct()

    // Cross join operation
    // TODO find a better way
    val allPairsDF = itemIDs.join(userIDs)
    val notRatedPairsDF = allPairsDF
      .except(ratingsDF.select(userIdCol, itemIdCol))

    val predictedRatingsDF = model.transform(notRatedPairsDF).filter(s"prediction > $minimumPositiveRating")

    // print 1000 to console
    predictedRatingsDF.show(1000)

    // comment table creation when running the second time TODO fix
    predictedRatingsDF.createCassandraTable(
      keyspace,
      cfPredictionsTable,
      partitionKeyColumns = Some(Seq("userid", "itemid", "prediction"))
    )

    predictedRatingsDF.write.mode("overwrite").cassandraFormat(cfPredictionsTable, keyspace).save()
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
