package gr.ml.analytics.service

import gr.ml.analytics.entities.Movie

trait MoviesService {

  /**
    * Get movie by id
    * @param id movie id
    * @return [[Movie]] entity
    */
  def get(id: Long): Movie

  /**
    * Get multiple movies by their ids
    * @param ids list of ids
    * @return list of [[Movie]] entities
    */
  def get(ids: List[Long]): List[Movie]
}
