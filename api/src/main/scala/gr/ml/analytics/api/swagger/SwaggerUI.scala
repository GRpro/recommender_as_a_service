package gr.ml.analytics.api.swagger

import akka.http.scaladsl.server.Directives

class SwaggerUI extends Directives {
  val site =
    path("swagger") { getFromResource("swagger/index.html") } ~
      getFromResourceDirectory("swagger")
}
