package gr.ml.analytics.service

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
  val bothRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small").toAbsolutePath.toString + File.separator + "*ratings.csv"
  val moviesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString
  val moviesWithFeaturesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies-with-features.csv").toAbsolutePath.toString
  val ratingsWithFeaturesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings-with-features.csv").toAbsolutePath.toString
  val ratingsWithFeaturesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "ratings-with-features-libsvm-user-%s.txt").toAbsolutePath.toString
  val ratingsSmallPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings-small.csv").toAbsolutePath.toString
  val allMoviesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "all-movies.txt").toAbsolutePath.toString
}
