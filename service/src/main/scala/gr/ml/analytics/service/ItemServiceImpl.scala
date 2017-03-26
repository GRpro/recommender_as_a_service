package gr.ml.analytics.service

class ItemServiceImpl extends ItemService {
  /**
    * @inheritdoc
    */
  override def get(id: Int): Item = {
    println(s"Get movie by id $id")
    null
  }

  /**
    * @inheritdoc
    */
  override def get(ids: List[Int]): List[Item] = {
    println(s"Get movies by ids $ids")
    null
  }
}
