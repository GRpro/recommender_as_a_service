package gr.ml.analytics

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import gr.ml.analytics.api.{ItemsAPI, RecommenderAPI}
import gr.ml.analytics.service.{ItemService, ItemServiceImpl, RecommenderService, RecommenderServiceImpl}

import scala.io.StdIn


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

  implicit val system = ActorSystem("recommendation-service")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  val log = Logging(system, getClass)

  // create services
  val recommenderService: RecommenderService = new RecommenderServiceImpl()
  var itemsService: ItemService = new ItemServiceImpl()

  // create apis
  val recommenderApi = new RecommenderAPI(recommenderService)
  val itemsApi = new ItemsAPI(itemsService)

  val recommenderAPIBindingFuture = Http().bindAndHandle(recommenderApi.route,
    interface = config.getString(serviceRecommenderListenerIface),
    port = config.getInt(serviceRecommenderListenerPort))

  val itemsAPIBindingFuture = Http().bindAndHandle(itemsApi.route,
    interface = config.getString(serviceItemsListenerIface),
    port = config.getInt(serviceItemsListenerPort))

  StdIn.readLine() // let it run until user presses return

  List(recommenderAPIBindingFuture, itemsAPIBindingFuture)
    .foreach(_.flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ system.terminate())) // and shutdown when done

  log.info("Stopped")
}
