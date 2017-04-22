package gr.ml.analytics

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import Configuration._
import gr.ml.analytics.api.{ItemsAPI, RatingsAPI, RecommenderAPI, SchemasAPI}
import gr.ml.analytics.cassandra.{CassandraConnector, InputDatabase}
import gr.ml.analytics.client.SchemasAPIClient
import gr.ml.analytics.service._
import akka.http.scaladsl.server.Directives._

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
  val schemasService: SchemaService = new SchemaServiceImpl(inputDatabase)
  val schemasClient: SchemasAPIClient = new SchemasAPIClient(serviceClientURI)

  val recommenderService: RecommenderService = new RecommenderServiceImpl(inputDatabase)
  var itemsService: ItemService = new ItemServiceImpl(inputDatabase, schemasClient)
  val ratingsService: RatingService = new RatingServiceImpl(inputDatabase)

  // create apis
  val recommenderApi = new RecommenderAPI(recommenderService)
  val itemsApi = new ItemsAPI(itemsService)
  val schemasApi = new SchemasAPI(schemasService)
  val ratingsApi = new RatingsAPI(ratingsService)

  val recommenderAPIBindingFuture = Http().bindAndHandle(
    recommenderApi.route ~ itemsApi.route ~ schemasApi.route ~ ratingsApi.route,
    interface = serviceListenerInterface,
    port = serviceListenerPort)

  StdIn.readLine() // let it run until user presses return

  List(recommenderAPIBindingFuture)
    .foreach(_.flatMap(_.unbind()) // trigger unbinding from the port
    .onComplete(_ ⇒ system.terminate())) // and shutdown when done

  log.info("Stopped")
}
