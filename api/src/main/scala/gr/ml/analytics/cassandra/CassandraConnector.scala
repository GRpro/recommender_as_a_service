package gr.ml.analytics.cassandra

import com.datastax.driver.core.{PoolingOptions, SocketOptions}
import com.outworkers.phantom.connectors.{CassandraConnection, ContactPoints}

object CassandraConnector {

  def apply(hosts: List[String],
            keyspace: String,
            username: Option[String],
            password: Option[String]): CassandraConnector =
    new CassandraConnector(hosts, keyspace, username, password)
}

class CassandraConnector(hosts: List[String],
                         keyspace: String,
                         username: Option[String] = None,
                         password: Option[String] = None) {

  /**
    * Create a connector with the ability to connects to
    * multiple hosts in a secured cluster
    */
  lazy val connector: CassandraConnection = {
    val connector = ContactPoints(hosts)
    if (username.isDefined && password.isDefined)
      connector
        .withClusterBuilder(
          _.withCredentials(username.get, password.get)
            .withSocketOptions(
              new SocketOptions()
                .setReadTimeoutMillis(5000)
                .setConnectTimeoutMillis(20000))
            .withPoolingOptions(
              new PoolingOptions()
                .setMaxQueueSize(100000)
                .setPoolTimeoutMillis(20000)))
        .keySpace(keyspace)
    else
      connector
        .keySpace(keyspace)
  }

}
