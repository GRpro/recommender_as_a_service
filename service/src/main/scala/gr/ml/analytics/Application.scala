package gr.ml.analytics


import akka.actor.{ActorSystem, Props}
import akka.event.Logging
import akka.io.IO
import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.api.{ItemsAPI, RecommenderAPI}
import gr.ml.analytics.service.{ItemService, ItemServiceImpl, RecommenderService, RecommenderServiceImpl}
import spray.can.Http


trait ConfigKeys {
  val serviceItemsListenerIface = "service.items.listener.iface"
  val serviceItemsListenerPort = "service.items.listener.port"

  val serviceRecommenderListenerIface = "service.recommender.listener.iface"
  val serviceRecommenderListenerPort = "service.recommender.listener.port"
}


/**
  * Application entry point
  */
object Application extends App with ConfigKeys {

  val config: Config = ConfigFactory.load("application.conf")

  // ActorSystem to host application in
  implicit val system = ActorSystem("recommendation-service")
  val log = Logging(system, getClass)

  // create services
  val recommenderService: RecommenderService = new RecommenderServiceImpl()
  var itemsService: ItemService = new ItemServiceImpl()

  // create apis
  val recommenderApi = system.actorOf(Props[RecommenderAPI](new RecommenderAPI(recommenderService)), "recommender-api")
  val moviesApi = system.actorOf(Props[ItemsAPI](new ItemsAPI(itemsService)), "movies-api")

  IO(Http) ! Http.Bind(moviesApi,
    interface = config.getString(serviceItemsListenerIface),
    port = config.getInt(serviceItemsListenerPort))

  IO(Http) ! Http.Bind(recommenderApi,
    interface = config.getString(serviceRecommenderListenerIface),
    port = config.getInt(serviceRecommenderListenerPort))
}
