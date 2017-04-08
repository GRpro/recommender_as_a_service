package gr.ml.analytics.service

trait ItemService {

  /**
    * Get item by id
    * @param id item id
    * @return item entity
    */
  def get(id: Int): Item

  /**
    * Get multiple items by their ids
    * @param ids list of ids
    * @return list of items
    */
  def get(ids: List[Int]): List[Item]
}
