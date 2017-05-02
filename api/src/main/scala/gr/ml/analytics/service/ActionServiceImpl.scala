package gr.ml.analytics.service

import gr.ml.analytics.cassandra.{Action, CassandraStorage}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class ActionServiceImpl(val inputDatabase: CassandraStorage) extends ActionService {

  private val actionModel = inputDatabase.actionsModel

  override def getByName(name: String): Future[Option[Action]] = actionModel.getById(name)

  override def getAll(): Future[List[Action]] = actionModel.getAll()

  override def create(action: Action): Future[Option[Action]] = {
    getByName(action.name).map {
      case Some(_) =>
        // already exists
        None
      case None =>
        actionModel.store(action)
        Some(action)
    }

  }
}
