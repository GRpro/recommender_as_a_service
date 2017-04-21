package gr.ml.analytics.service
import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

class CassandraSource(val sparkSession: SparkSession, val config: Config) extends Source {

  private val schemaId: Int = config.getInt("items_schema_id")

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val ratingsTable: String = config.getString("cassandra.ratings_table")
  private val itemsTable: String = config.getString("cassandra.items_table_prefix") + schemaId
  private val schemasTable: String = config.getString("cassandra.schemas_table")

  private val keyCol = "key"
  private val userIdCol = "userid"
  private val itemIdCol = "itemid"
  private val ratingCol = "rating"
  private val timestampCol = "timestamp"
  private val predictionCol = "prediction"
  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

//  spark.conf.set("spark.sql.crossJoin.enabled", "true")

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

  override def getPredictionsForUser(userId: Int, table: String): DataFrame = {
    spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> table, "keyspace" -> keyspace))
    .load()
    .select(keyCol, userIdCol, itemIdCol, predictionCol)
    .where(col(userIdCol) === userId)
  }

  private lazy val schema: Map[String, Any] = CassandraConnector(sparkSession.sparkContext).withSessionDo[Map[String, Any]] { session =>

    def convertJson(itemString: String): Map[String, Any] = {
      val json = JSON.parseFull(itemString)
      json match {
        case Some(item: Map[String, Any]) => item
        case None => throw new RuntimeException("item validation error")
      }
    }

    val res = session.execute(s"SELECT JSON * FROM $keyspace.$schemasTable WHERE schemaId = $schemaId").one()
    val json = convertJson(res.get("[json]", classOf[String]))
    convertJson(json("jsonschema").asInstanceOf[String])
  }

  private def getUserIdsForLastDF(seconds: Long): DataFrame = {
    val userIdsDF = ratingsDS.filter($"timestamp" > System.currentTimeMillis / 1000 - seconds)
      .select(userIdCol).distinct()
    userIdsDF
  }

  override def all: DataFrame = ratingsDS


  override def getUserIdsForLastNSeconds(seconds: Long): Set[Int] = {
    val userIdsDF = getUserIdsForLastDF(seconds)

    val userIdsSet = userIdsDF.collect()
      .map(r => r.getInt(0))
      .toSet
    userIdsSet

    ratingsDS.select(userIdCol).take(5).map(r => r.getInt(0)).toSet
  }


  override def getUserItemPairsToRate(userId: Int): DataFrame = {
    val itemIdsNotToIncludeDF = ratingsDS.filter($"userid" === userId).select("itemid") // 4 secs
    val itemIdsNotToIncludeSet = itemIdsNotToIncludeDF.collect()
      .map(r=>r.getInt(0))
      .toSet.toList
    val itemsIdsToRate = itemIDsDS.filter(!$"itemid".isin(itemIdsNotToIncludeSet: _*)) // quick
    val notRatedPairsDF = itemsIdsToRate.withColumn("userid", lit(userId))
      .select(col("itemid").as("itemId"), col("userid").as("userId"))
    notRatedPairsDF
  }


  override def getAllItemsAndFeatures(): DataFrame = {

    val allowedTypes = Set("double", "float")

    val idColumnName = schema("id").asInstanceOf[Map[String, String]]("name")
    val featureColumnNames = schema("features").asInstanceOf[List[Map[String, String]]]
      .filter((colDescription: Map[String, Any]) => allowedTypes.contains(colDescription("type").asInstanceOf[String].toLowerCase))
      .map(colDescription => colDescription("name"))


    val itemsRowDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> itemsTable, "keyspace" -> keyspace))
      .load()
      .select(idColumnName, featureColumnNames:_*)

    val itemsDF = itemsRowDF.map(row => {
      val id = row.getInt(0)
      val featureValues = (1 until row.length).map(idx => row.getDouble(idx))
      val featureValuesArr: Array[Double] = featureValues.toArray
      (id, Vectors.dense(featureValuesArr))
    }).toDF("itemid", "features")

    itemsDF

    // the format of libsvm
    // see http://stackoverflow.com/questions/41416291/how-to-prepare-data-into-a-libsvm-format-from-dataframe
  }
}
