import sbt.Keys._

val commonSettings = Seq(
  organization := "gr.ml.analytics",
  /* Get short version of the last commit id */
  //version := s"${Process("git log --pretty=format:'%h' -n 1").lines.head}",
  version := "0.1",
  scalaVersion := "2.11.8",
  javacOptions ++= Seq("-encoding", "UTF-8")
)

/**
  * Setting to generate a class with module-specific properties during compilation
  */
val buildInfoSettings = Seq(
  sourceGenerators in Compile <+= (sourceManaged in Compile, version, name) map { (d, v, n) =>
    val file = d / "info.scala"
    val pkg = "gr.ml.analytics"
    IO.write(file, s"""package $pkg
                     |class BuildInfo {
                     |  val info = Map[String, String](
                     |    "name" -> "%s",
                     |    "version" -> "%s"
                     |    )
                     |}
                     |""".stripMargin.format(n, v))
    Seq(file)
  }
)

// dependencies configuration

val sprayVersion = "1.3.2"
val akkaVersion = "2.4.2"
val phantomVersion = "2.1.3"
val sparkVersion = "2.0.1"

resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/", "Spray Repository" at "http://repo.spray.io")


val cassandraIntegrationDependencies = Seq(
  "io.spray" %% "spray-routing-shapeless2" % sprayVersion,
  "com.outworkers" %% "phantom-dsl" % phantomVersion
)

val sprayDependencies = Seq(
  "io.spray" %% "spray-can" % sprayVersion,
  "io.spray" %% "spray-routing" % sprayVersion,
  "io.spray" %% "spray-json" % sprayVersion,
  "io.spray" %% "spray-client" % sprayVersion,
  "io.spray" %% "spray-testkit" % sprayVersion % "test"
)

val akkaDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion
)

// module structure configuration

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "recommendation"
  )
  .aggregate(service)


lazy val api = project.in(file("api"))
  .settings(commonSettings: _*)
  .settings(
    name := "recommendation-api"
  )

lazy val service = project.in(file("service"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommendation-service"
  )
  .settings(
    libraryDependencies ++= sparkDependencies,
    libraryDependencies ++= sprayDependencies,
    libraryDependencies ++= akkaDependencies,
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "org.specs2" %% "specs2" % "2.3.13" % "test",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  )
  .dependsOn(api)


// Add any command aliases that may be useful as shortcuts
addCommandAlias("cc", ";clean;compile")
