package gr.ml.analytics.service

import gr.ml.analytics.cassandra.Action

import scala.concurrent.Future


trait ActionService {

  def getByName(name: String): Future[Option[Action]]

  def getAll(): Future[List[Action]]

  def create(action: Action): Future[Option[Action]]

}
