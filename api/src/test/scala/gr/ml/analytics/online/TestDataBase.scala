package gr.ml.analytics.online

import com.outworkers.phantom.connectors.ContactPoints
import gr.ml.analytics.cassandra.CassandraStorage

object TestConnector {
  val connector = ContactPoints(Seq("localhost")).keySpace("test_keyspace")
}

object TestDb extends CassandraStorage(TestConnector.connector)