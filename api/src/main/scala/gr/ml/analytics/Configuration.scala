package gr.ml.analytics

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

object Configuration {

  val config: Config = ConfigFactory.load("application.conf")

  val serviceListenerInterface: String = config.getString("service.listener.iface")
  val serviceListenerPort: Int = config.getInt("service.listener.port")
  val serviceClientURI: String = config.getString("service.rest")

  val cassandraHosts: List[String] = config.getStringList("cassandra.hosts").toList
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraUsername: String = config.getString("cassandra.username")
  val cassandraPassword: String = config.getString("cassandra.password")
}
