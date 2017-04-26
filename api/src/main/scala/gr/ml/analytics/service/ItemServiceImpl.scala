package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.domain.{Item, Schema}

import scala.concurrent.Future
import scala.util.parsing.json.JSONObject
import scala.concurrent.ExecutionContext.Implicits.global

class ItemServiceImpl(val inputDatabase: InputDatabase) extends ItemService with LazyLogging {

  /**
    * @inheritdoc
    */
  override def get(schemaId: Int, itemId: Int): Future[Option[Item]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)
        val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schemaMap)

        // retrieve content in json format
        val tableName = Util.itemsTableName(schemaId)
        val query = s"SELECT JSON * FROM ${inputDatabase.ratingModel.keySpace}.$tableName WHERE $idName = $itemId"
        val res = inputDatabase.connector.session.execute(query).one()
        val item: Item = Util.convertJson(res.get("[json]", classOf[String]))

        Some(item)
      case None => None
    }
  }

  /**
    * @inheritdoc
    */
  override def get(schemaId: Int, itemIds: List[Int]): Future[List[Option[Item]]] = {
    Future.sequence(itemIds.map(itemId => get(schemaId, itemId)))
  }

  /**
    * @inheritdoc
    */
  override def save(schemaId: Int, item: Item): Future[Option[Int]] = {
    val schemaFuture = inputDatabase.schemasModel.getOne(schemaId)

    schemaFuture.map {
      case Some(schema: Schema) =>
        val schemaMap: Map[String, Any] = schema.jsonSchema
        val (idName, idType) = Util.extractIdMetadata(schemaMap)
        val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schemaMap)


        // TODO implement proper mechanism to escape characters which have special meaning for Cassandra
        val json = JSONObject(item).toString().replace("'", "")
        val tableName = Util.itemsTableName(schemaId)
        val query = s"INSERT INTO ${inputDatabase.ratingModel.keySpace}.$tableName JSON '$json'"
        val res = inputDatabase.connector.session.execute(query).wasApplied()

        logger.info(s"Creating item: '$query' Result: $res")

        Some(item(idName).asInstanceOf[BigDecimal].toInt)
      case None =>
        None
    }
  }
}
