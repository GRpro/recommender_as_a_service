package gr.ml.analytics.online

import com.outworkers.phantom.connectors.{CassandraConnection, ContactPoints}
import gr.ml.analytics.cassandra.CassandraStorage
import gr.ml.analytics.online.cassandra._
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.specs2.concurrent.ExecutionEnv
import org.specs2.mutable._
import org.specs2.specification.BeforeAfterAll

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class ItemItemSpec extends Specification with BeforeAfterAll {

  sequential

  type EE = ExecutionEnv

  EmbeddedCassandraServerHelper.startEmbeddedCassandra(EmbeddedCassandraServerHelper.CASSANDRA_RNDPORT_YML_FILE, 20000L)

  val port = EmbeddedCassandraServerHelper.getNativeTransportPort
  val host = EmbeddedCassandraServerHelper.getHost

  lazy val connector: CassandraConnection = ContactPoints(Seq(host), port).keySpace("cassandra_test")

  object TestDb extends CassandraStorage(connector)

  val testDb = TestDb

  Await.ready(testDb.createAsync(), 20.seconds)


  val recommender = new ItemItemRecommender(testDb)


  def beforeAll(): Unit = {

    val weightsMap = Map(
      "display" -> 0,
      "hover" -> 1,
      "click" -> 2,
      "like" -> 3,
      "share" -> 4,
      "buy" -> 5
    )



    val events =
      Interaction("u1", "i1", "buy", System.currentTimeMillis / 1000) ::
      Interaction("u1", "i2", "like", System.currentTimeMillis / 1000) ::
      Interaction("u1", "i4", "click", System.currentTimeMillis / 1000) ::
      Interaction("u2", "i2", "share", System.currentTimeMillis / 1000) ::
      Interaction("u2", "i3", "like", System.currentTimeMillis / 1000) ::
      Interaction("u2", "i5", "hover", System.currentTimeMillis / 1000) ::
      Interaction("u3", "i1", "click", System.currentTimeMillis / 1000) ::
      Interaction("u3", "i4", "click", System.currentTimeMillis / 1000) ::
      Interaction("u4", "i2", "like", System.currentTimeMillis / 1000) ::
      Interaction("u4", "i3", "buy", System.currentTimeMillis / 1000) ::
      Interaction("u4", "i5", "click", System.currentTimeMillis / 1000) ::
      Interaction("u5", "i1", "share", System.currentTimeMillis / 1000) ::
      Interaction("u5", "i2", "hover", System.currentTimeMillis / 1000) ::
      Interaction("u5", "i4", "buy", System.currentTimeMillis / 1000) ::
      Nil

    for (event <- events) {
      recommender.learn(event, weightsMap(event.action))
      Thread.sleep(1000)
    }

  }

  def afterAll(): Unit = {
    Await.ready(testDb.truncateAsync(), 10.seconds)
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
  }


  "Item-Item Recommender correctly" >> {

    "tracks events" >> {

      "for item i1" >> { implicit ee: EE =>
        val similarity = testDb.similarities.getById("i1")
        Await.ready(similarity, 3.seconds)
        similarity.value.get.get must beEqualTo(Seq(Similarity("i1", "i4", 0.8040302522073697), Similarity("i1", "i2", 0.36363636363636365)))
      }

      "for item i2" >> { implicit ee: EE =>
        val similarity = testDb.similarities.getById("i2")
        Await.ready(similarity, 3.seconds)
        similarity.value.get.get must beEqualTo(Seq(Similarity("i2","i3",0.6708203932499369), Similarity("i2","i5",0.5477225575051661), Similarity("i2","i1",0.36363636363636365), Similarity("i2","i4",0.30151134457776363)))
      }

      "for item i3" >> { implicit ee: EE =>
        val similarity = testDb.similarities.getById("i3")
        Await.ready(similarity, 3.seconds)
        similarity.value.get.get must beEqualTo(Seq(Similarity("i3","i2",0.6708203932499369), Similarity("i3","i5",0.6123724356957945)))
      }

      "for item i4" >> { implicit ee: EE =>
        val similarity = testDb.similarities.getById("i4")
        Await.ready(similarity, 3.seconds)
        similarity.value.get.get must beEqualTo(Seq(Similarity("i4", "i1", 0.8040302522073697), Similarity("i4", "i2", 0.30151134457776363)))
      }

      "for item i5" >> { implicit ee: EE =>
        val similarity = testDb.similarities.getById("i5")
        Await.ready(similarity, 3.seconds)
        similarity.value.get.get must beEqualTo(Seq(Similarity("i5", "i3", 0.6123724356957945), Similarity("i5", "i2", 0.5477225575051661)))
      }
    }


    "gives recommendations" >> {

      "for user u1" >> { implicit ee: EE =>
        val recommendations = recommender.getRecommendations("u1")
        Await.ready(recommendations, 3.seconds)
        recommendations.value.get.get must beEqualTo(Seq(("i5", 3), ("i3", 3)))
      }

      "for user u2" >> { implicit ee: EE =>
        val recommendations = recommender.getRecommendations("u2")
        Await.ready(recommendations, 3.seconds)
        recommendations.value.get.get must beEqualTo(Seq(("i4", 4), ("i1", 4)))
      }

      "for user u3" >> { implicit ee: EE =>
        val recommendations = recommender.getRecommendations("u3")
        Await.ready(recommendations, 3.seconds)
        recommendations.value.get.get must beEqualTo(Seq(("i2", 2)))
      }

      "for user u4" >> { implicit ee: EE =>
        val recommendations = recommender.getRecommendations("u4")
        Await.ready(recommendations, 3.seconds)
        recommendations.value.get.get must beEqualTo(Seq(("i4", 3), ("i1", 2.9999999999999996)))
      }

      "for user u5" >> { implicit ee: EE =>
        val recommendations = recommender.getRecommendations("u5")
        Await.ready(recommendations, 3.seconds)
        recommendations.value.get.get must beEqualTo(Seq(("i5", 1), ("i3", 1)))
      }

    }

  }

}

