package gr.ml.analytics.service

import gr.ml.analytics.domain.Item

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

  /**
    * Stores item
    * @param id unique item id
    * @param features map of name/value features for the item
    */
  def save(id: Int, features: Map[String, String])
}
