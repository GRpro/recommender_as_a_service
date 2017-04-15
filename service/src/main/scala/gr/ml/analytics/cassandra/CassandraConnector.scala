package gr.ml.analytics.cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

object CassandraUtil {

  def setCassandraProperties(sparkSession: SparkSession, config: Config): SparkSession = {
    val cassandraHost: String = config.getString("cassandra.bootstrap_host")

    sparkSession.setCassandraConf(CassandraConnectorConf.ConnectionHostParam.option(cassandraHost))
  }

}
