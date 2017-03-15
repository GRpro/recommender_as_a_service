package gr.ml.analytics.service
import gr.ml.analytics.entities.Movie

class MoviesServiceImpl extends MoviesService {
  /**
    * Get movie by id
    *
    * @param id movie id
    * @return [[Movie]] entity
    */
  override def get(id: Long): Movie = {
    println(s"Get movie by id $id")
    null
  }

  /**
    * Get multiple movies by their ids
    *
    * @param ids list of ids
    * @return list of [[Movie]] entities
    */
  override def get(ids: List[Long]): List[Movie] = {
    println(s"Get movies by ids $ids")
    null
  }
}
