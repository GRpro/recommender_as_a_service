package gr.ml.analytics.online

import com.typesafe.scalalogging.LazyLogging
import gr.ml.analytics.cassandra.CassandraStorage
import gr.ml.analytics.online.cassandra.{Similarity, SimilarityIndex, User}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.math.{Ordering, min, sqrt}
import scala.util.{Failure, Success}

class ItemItemRecommender(storage: CassandraStorage) extends LazyLogging {

  def learn(event: Interaction, weight: Double): Unit = {
    val userId = event.userId
    val itemId = event.itemId

    val user = storage.users.getById(userId)

    user.onComplete {
      case Success(Some(u)) =>
        val currentWeight: Double = u.items.getOrElse(itemId, 0)
        if (currentWeight < weight) {
          storage.users.updateUser(User(u.id, u.items + (itemId -> weight)))
          recalculateSimilarity(u, itemId, currentWeight, weight)
        }

      case Success(None) =>
        val f = saveNewUser(userId, itemId, weight)
        Await.ready(f, 3.seconds)

      case Failure(message) => logger.warn(s"No user with id $userId. $message")
    }

  }

  private def recalculateSimilarity(user: User, itemId: String, currentWeight: Double, newWeight: Double): Unit = {
    println("RECALCULATING SIMILARITY")
    val currentItemCount = storage.itemCounts.getById(itemId)

    def callback(currentItemCount: Double): Unit = {
      val itemCountDelta = newWeight - currentWeight
      updateItemCount(itemId, itemCountDelta)
      val newItemCount = currentItemCount + itemCountDelta
      for {
        anotherItem <- user.items if anotherItem._1 != itemId
      } updatePair(itemId, currentWeight, newWeight, newItemCount, anotherItem)
    }

    currentItemCount.onComplete{
      case Success(Some(item)) => callback(item.count)
      case Success(None) => callback(0)
      case Failure(message) => println(message)
    }
  }


  private def updateItemCount(itemId: String, deltaWeight: Double): Future[_] = {
    storage.itemCounts.incrementCount(itemId, deltaWeight)
  }

  private def updatePair(eventItemId: String, currentItemWeight: Double, newItemWeight: Double,
                 newItemCount: Double, anotherItem: (String, Double)): Unit = {
    val anotherItemId = anotherItem._1
    val anotherItemWeight = anotherItem._2
    updatePairCount(eventItemId, currentItemWeight, newItemWeight, newItemCount, anotherItemId, anotherItemWeight)
  }

  private def getPairId(firstItemId: String, secondItemId: String): String =
    Ordering[String].min(firstItemId, secondItemId) + "_" + Ordering[String].max(firstItemId, secondItemId)

  private def updatePairCount(eventItemId: String, currentItemWeight: Double, newItemWeight: Double,
                      newItemCount: Double, anotherItemId: String, anotherItemWeight: Double): Unit = {

    val deltaCoRating = {
      if (currentItemWeight == 0) min(newItemWeight, anotherItemWeight)
      else {
        (currentItemWeight < anotherItemWeight, newItemWeight < anotherItemWeight) match {
          case (true, true) => newItemWeight - currentItemWeight
          case (true, false) => anotherItemWeight - currentItemWeight
          case (false, _) => 0
        }
      }
    }

    val pairId = getPairId(eventItemId, anotherItemId)
    val currentPairCount = storage.pairCounts.getById(pairId)

    def callback(initCount: Double): Unit = {
      if (deltaCoRating != 0) {
        storage.pairCounts.incrementCount(pairId, deltaCoRating)
        updateSimilarity(eventItemId, newItemCount, anotherItemId, initCount + deltaCoRating)
      }
      else updateSimilarity(eventItemId, newItemCount, anotherItemId, initCount + deltaCoRating)
    }

    currentPairCount.onComplete{
      case Success(Some(count)) => callback(count.count)
      case Success(None) => callback(0)
      case Failure(message) => println(message)
    }
  }


  private def updateSimilarity(firstItem: String, newItemCount: Double, secondItem: String, pairCount: Double): Unit = {
    val secondItemCount = storage.itemCounts.getById(secondItem)
    println("PAIRCOUNT: " + pairCount + ", NEWITEMCOUNT: " + newItemCount)
    for {
      Some(secondItem) <- secondItemCount
      similarity: Double = pairCount / (sqrt(newItemCount) * sqrt(secondItem.count))
    } saveSimilarity(firstItem, secondItem.itemId, similarity)
  }

  private def saveSimilarity(firstItem: String, secondItem: String, similarity: Double): Unit = {
    println("SAVING SIMILARITY")
    val pairId = getPairId(firstItem, secondItem)
    val similarityIndex = storage.similaritiesIndex.getById(pairId)
    similarityIndex.onComplete{
      case Success(Some(currentSimilarity)) =>
        storage.similarities.deleteRow(Similarity(firstItem, secondItem, currentSimilarity.similarity))
        storage.similarities.deleteRow(Similarity(secondItem, firstItem, currentSimilarity.similarity))
        storage.similarities.store(Similarity(firstItem, secondItem, similarity))
        storage.similarities.store(Similarity(secondItem, firstItem, similarity))
        storage.similaritiesIndex.store(SimilarityIndex(pairId, similarity))

      case Success(None) =>
        storage.similarities.store(Similarity(firstItem, secondItem, similarity))
        storage.similarities.store(Similarity(secondItem, firstItem, similarity))
        storage.similaritiesIndex.store(SimilarityIndex(pairId, similarity))

      case Failure(message) => println(message)
    }
  }

  private def saveNewUser(userId: String, itemId: String, weight: Double): Future[_] = {
    println("SAVING NEW USER")
    val f1 = storage.users.store(User(userId, Map(itemId -> weight)))
    val f2 = updateItemCount(itemId, weight)
    Future.sequence(Seq(f1, f2))
  }

  def getSimilarItems(itemId: String, limit: Int = 10): Future[Seq[Similarity]] = {
    val similarities = storage.similarities.getById(itemId, limit)
    similarities.map(simList => Similarity(itemId, itemId, 1) +: simList)
  }

  def getRecommendations(userId: String, limit: Int = 10): Future[Seq[(String, Double)]] = {

    type UserItem = (String, Double)

    def getUserItems(user: Option[User]): Seq[UserItem] = user match {
      case Some(u) => u.items.toList
      case None => Nil
    }

    def getItemSimilarity(userItems: Seq[UserItem]): Seq[(UserItem, Future[Seq[Similarity]])] = {
      userItems.map(userItem => (userItem, storage.similarities.getById(userItem._1, limit)))
    }

    def reformatSimilarity(similarities: Seq[(UserItem, Future[Seq[Similarity]])]): Future[Seq[(UserItem, Seq[Similarity])]] = {
      val (items, sims) = similarities.unzip
      val sims1 = Future.sequence(sims)
      sims1.map(x => items.zip(x))
    }

    def getSimilaritySummands(similarities: Seq[(UserItem, Seq[Similarity])]): Seq[(String, Double, Double)] = {
      for {
        (item, sims) <- similarities
        sim <- sims
      } yield (sim.anotherItemId, sim.similarity * item._2, sim.similarity)
    }

    def getSimilaritySum(ratingsRaw: Seq[(String, Double, Double)]): Seq[(String, Double)] = {
      ratingsRaw.groupBy(_._1).map {
        case (x, y) => (x, y.map(_._2).sum / y.map(_._3).sum)
      }.toSeq
    }

    for {
      items <- storage.users.getById(userId).map(getUserItems)
      recommendations <- reformatSimilarity(getItemSimilarity(items)).map(getSimilaritySummands).map(getSimilaritySum)
    } yield recommendations.filter(rec => !items.map(_._1).contains(rec._1)).sortWith(_._2 > _._2)
  }


}