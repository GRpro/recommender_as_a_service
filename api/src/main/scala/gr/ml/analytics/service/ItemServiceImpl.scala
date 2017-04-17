package gr.ml.analytics.service

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.InputDatabase
import gr.ml.analytics.client.SchemasAPIClient
import gr.ml.analytics.domain.Item

import scala.util.parsing.json.JSONObject

class ItemServiceImpl(val inputDatabase: InputDatabase, val schemasClient: SchemasAPIClient) extends ItemService with LazyLogging {


  /**
    * @inheritdoc
    */
  override def get(schemaId: Int, itemId: Int): Item = {
    val tableName = Util.itemsTableName(schemaId)
    val schema: Map[String, Any] = Util.schemaToMap(schemasClient.get(schemaId))
    val (idName, idType) = Util.extractIdMetadata(schema)
    val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schema)

    // retrieve content in json format
    val query = s"SELECT JSON * FROM ${inputDatabase.ratingModel.keySpace}.$tableName WHERE $idName = $itemId"

    val res = inputDatabase.connector.session.execute(query).one()

    val json = Util.convertJson(res.get("[json]", classOf[String]))

    json
  }

  /**
    * @inheritdoc
    */
  override def get(schemaId: Int, itemIds: List[Int]): List[Item] = {
    itemIds.map(itemId => get(schemaId, itemId))
  }

  /**
    * @inheritdoc
    */
  override def save(schemaId: Int, item: Item): Int = {
    val tableName = Util.itemsTableName(schemaId)

    val schema: Map[String, Any] = Util.schemaToMap(schemasClient.get(schemaId))

    val (idName, idType) = Util.extractIdMetadata(schema)
    val featureColumnsInfo: List[Map[String, String]] = Util.extractFeaturesMetadata(schema)

    // TODO implement proper mechanism to escape characters which have special meaning for Cassandra
    val json = JSONObject(item).toString().replace("'", "")
    val query = s"INSERT INTO ${inputDatabase.ratingModel.keySpace}.$tableName JSON '$json'"

    val res = inputDatabase.connector.session.execute(query).wasApplied()

    logger.info(s"Creating item: '$query' Result: $res")

    item(idName).asInstanceOf[BigDecimal].toInt
  }
}
