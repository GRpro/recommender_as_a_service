import sbt.Keys._

val commonSettings = Seq(
  organization := "gr.ml.analytics",
  version := s"${Process("git describe --tags").lines.head}",
  scalaVersion := "2.11.8",
  javacOptions ++= Seq("-encoding", "UTF-8")
)

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

// root
lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .settings(
    name := "recommendation"
  )
  .aggregate(webapp, service)


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
    libraryDependencies ++= {
      val sparkVersion = "2.0.1"
      Seq(
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion
      )
    },
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.7",
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.2",
      "com.typesafe.akka" %% "akka-remote" % "2.4.2"
      ),
    libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.1" % "test"
  )
  .dependsOn(api)

lazy val webapp = project.in(file("webapp"))
  .enablePlugins(PlayScala)
  .settings(commonSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    name := "recommendation-webapp"
  )
  .dependsOn(api)





