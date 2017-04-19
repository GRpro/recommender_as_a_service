package gr.ml.analytics.service

import com.datastax.spark.connector.cql.CassandraConnector
import com.typesafe.config.Config
import gr.ml.analytics.cassandra.CassandraUtil
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

class CassandraSink(val sparkSession: SparkSession, val config: Config, val tableName: String) extends Sink {

  private val keyspace: String = config.getString("cassandra.keyspace")
  private val spark = CassandraUtil.setCassandraProperties(sparkSession, config)

  CassandraConnector(sparkSession.sparkContext).withSessionDo { session =>
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute(s"CREATE TABLE IF NOT EXISTS $keyspace.$tableName (key text PRIMARY KEY, userid int, itemid int, prediction float)")
  }

  /**
    * @inheritdoc
    */
  override def store(predictions: DataFrame): Unit = {

    val normalized = predictions
      .select("userId", "itemId", "prediction")
      .toDF("userid", "itemid", "prediction").withColumn("key", concat(col("userid"), lit(":"), col("itemid")))

    normalized.show(1000)
    normalized
      .write.mode("overwrite")
      .cassandraFormat(tableName, keyspace)
      .save()
  }
}
