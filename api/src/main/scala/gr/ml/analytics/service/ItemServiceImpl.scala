package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.{CassandraConnector, InputDatabase}
import gr.ml.analytics.domain.Item

class ItemServiceImpl(cassandraConnector: => CassandraConnector) extends ItemService with LazyLogging {

  private lazy val itemModel = new InputDatabase(cassandraConnector.connector).itemModel

  /**
    * @inheritdoc
    */
  override def get(id: Int): Item = {
    println(s"Get movie by id $id")
    ???
  }

  /**
    * @inheritdoc
    */
  override def get(ids: List[Int]): List[Item] = {
    println(s"Get movies by ids $ids")
    ???
  }

  /**
    * @inheritdoc
    */
  override def save(id: Int, features: Map[String, String]): Unit = {
    val itemEntity = Item(id)
    itemModel.save(itemEntity)

    logger.info(s"saved $itemEntity")
  }
}
