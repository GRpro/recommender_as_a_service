package gr.ml.analytics

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import Configuration._
import gr.ml.analytics.api.{ItemsAPI, RecommenderAPI, SchemasAPI}
import gr.ml.analytics.cassandra.{CassandraConnector, InputDatabase}
import gr.ml.analytics.service._

import scala.io.StdIn





/**
  * Application entry point
  */
object Application extends App {

  implicit val system = ActorSystem("recommendation-service")
  implicit val materializer = ActorMaterializer()
  // needed for the future flatMap/onComplete in the end
  implicit val executionContext = system.dispatcher

  val log = Logging(system, getClass)


  def cassandraConnector = CassandraConnector(
    cassandraHosts,
    cassandraKeyspace,
    Some(cassandraUsername),
    Some(cassandraPassword))

  val inputDatabase = new InputDatabase(cassandraConnector.connector)

  // create services
  val recommenderService: RecommenderService = new RecommenderServiceImpl(inputDatabase)
  var itemsService: ItemService = new ItemServiceImpl(inputDatabase)
  val schemasService: SchemaService = new SchemaServiceImpl(inputDatabase)

  // create apis
  val recommenderApi = new RecommenderAPI(recommenderService)
  val itemsApi = new ItemsAPI(itemsService)
  val schemasApi = new SchemasAPI(schemasService)

  val recommenderAPIBindingFuture = Http().bindAndHandle(recommenderApi.route,
    interface = serviceRecommenderListenerInterface,
    port = serviceRecommenderListenerPort)

  val itemsAPIBindingFuture = Http().bindAndHandle(itemsApi.route,
    interface = serviceItemsListenerInterface,
    port = serviceItemsListenerPort)

  val schemasAPIBindingFuture = Http().bindAndHandle(schemasApi.route,
    interface = serviceSchemasListenerInterface,
    port = serviceSchemasListenerPort)

  StdIn.readLine() // let it run until user presses return

  List(recommenderAPIBindingFuture, itemsAPIBindingFuture)
    .foreach(_.flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ system.terminate())) // and shutdown when done

  log.info("Stopped")
}
