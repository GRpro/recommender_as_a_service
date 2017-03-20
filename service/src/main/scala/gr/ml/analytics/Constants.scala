package gr.ml.analytics

import java.io.File
import java.nio.file.Paths

/**
  * Common constants used by service
  */
trait Constants {
  val smallDatasetFileName: String = "ml-latest-small.zip"
  val smallDatasetUrl: String = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

  val datasetsDirectory: String = "data"
  val historicalRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val currentRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "current-ratings.csv").toAbsolutePath.toString
  val predictionsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "predictions.csv").toAbsolutePath.toString
  val modelPath: String = Paths.get(datasetsDirectory, "model").toAbsolutePath.toString
  val bothRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small").toAbsolutePath.toString + File.separator + "ratings.csv"
}
