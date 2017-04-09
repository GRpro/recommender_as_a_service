import sbt.Keys.{libraryDependencies, _}

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
    IO.write(file,
      s"""package $pkg
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

// dependency versions
val akkaVersion = "2.4.17"
val akkaHttpVersion = "10.0.5"
val phantomVersion = "2.1.3"
val sparkVersion = "2.0.1"

// module structure configuration

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "recommender-saas",
    description :=
      """
        |Root project of recommender as a service software which aggregates
        |set of modules related to the project.
      """.stripMargin
  )
  .aggregate(service, api, common)


lazy val common = project.in(file("common"))
  .settings(commonSettings: _*)
  .settings(
    name := "recommender-saas-common",
    description :=
      """
        |Common utilities and services shared across modules inside recommender as a service software.
      """.stripMargin
  )

lazy val api = project.in(file("api"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommender-saas-api",
    description :=
      """
        |REST API for recommender as a service software
      """.stripMargin
  )
  .settings(

    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
    ),

    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
    ),
//    libraryDependencies += "io.spray" %% "spray-routing-shapeless2" % "1.3.2",
      //    libraryDependencies += "com.outworkers" %% "phantom-dsl" % phantomVersion,
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    libraryDependencies += "org.specs2" %% "specs2" % "2.3.13" % "test",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  )
  .dependsOn(common)

lazy val service = project.in(file("service"))
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommender-saas-service",
    description :=
      """
        |Recommender service where all magic happens
      """.stripMargin
  )
  .settings(
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Spray Repository" at "http://repo.spray.io"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-mllib" % sparkVersion
    ),
//    libraryDependencies += "com.outworkers" %% "phantom-dsl" % phantomVersion,
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.0",
    libraryDependencies += "org.specs2" %% "specs2" % "2.3.13" % "test",
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  )
  .dependsOn(common)


// Add any command aliases that may be useful as shortcuts
addCommandAlias("cct", ";clean;compile;test")
