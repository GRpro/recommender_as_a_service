package gr.ml

package object analytics {
  /**
    * gr.ml.analytics.BuildInfo class is auto generated before
    * compilation phase by all other modules that depend on this one.
    */
  lazy val buildInfo = Class.forName("gr.ml.analytics.BuildInfo")
    .newInstance
    .asInstanceOf[{ val info: Map[String, String] }]
    .info

}
