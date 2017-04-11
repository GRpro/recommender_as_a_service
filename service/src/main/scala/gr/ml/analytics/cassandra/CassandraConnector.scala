package gr.ml.analytics.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

class CassandraConnector(val config: Config) {

  val cassandraHost: String = config.getString("cassandra.bootstrap_host")

  def setConnectionInfo(sparkSession: SparkSession): SparkSession = {
    sparkSession.setCassandraConf(CassandraConnectorConf.ConnectionHostParam.option(cassandraHost))
  }
}

object CassandraConnector {
  def apply(config: Config): CassandraConnector = new CassandraConnector(config)
}