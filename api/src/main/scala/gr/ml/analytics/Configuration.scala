package gr.ml.analytics

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._

object Configuration {

  val config: Config = ConfigFactory.load("application.conf")

  val serviceItemsListenerInterface: String = config.getString("service.items.listener.iface")
  val serviceItemsListenerPort: Int = config.getInt("service.items.listener.port")

  val serviceRecommenderListenerInterface: String = config.getString("service.recommender.listener.iface")
  val serviceRecommenderListenerPort: Int = config.getInt("service.recommender.listener.port")

  val serviceSchemasListenerInterface: String = config.getString("service.schemas.listener.iface")
  val serviceSchemasListenerPort: Int = config.getInt("service.schemas.listener.port")
  val serviceSchemasClientURI: String = config.getString("service.schemas.rest")

  val serviceRatingsListenerInterface: String = config.getString("service.ratings.listener.iface")
  val serviceRatingsListenerPort: Int = config.getInt("service.ratings.listener.port")

  val cassandraHosts: List[String] = config.getStringList("cassandra.host").toList
  val cassandraKeyspace: String = config.getString("cassandra.keyspace")
  val cassandraUsername: String = config.getString("cassandra.username")
  val cassandraPassword: String = config.getString("cassandra.password")
}
