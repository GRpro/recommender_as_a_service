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
  val ratingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val historicalRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString   // TODO remove
  val currentRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "current-ratings.csv").toAbsolutePath.toString // TODO remove
  val predictionsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "predictions.csv").toAbsolutePath.toString
  val modelPath: String = Paths.get(datasetsDirectory, "model", "collaborative").toAbsolutePath.toString
  val bothRatingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small").toAbsolutePath.toString + File.separator + "*ratings.csv" // TODO remove
  val moviesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString
  val moviesWithFeaturesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies-with-features.csv").toAbsolutePath.toString
  val ratingsWithFeaturesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings-with-features.csv").toAbsolutePath.toString
  val ratingsWithFeaturesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "ratings-with-features-libsvm-user-%s.txt").toAbsolutePath.toString
  val ratingsSmallPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings-small.csv").toAbsolutePath.toString
  val allMoviesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "all-movies.txt").toAbsolutePath.toString
  val collaborativePredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "collaborative-predictions", "collaborative-predictions-user-%s.csv").toAbsolutePath.toString
  val contentBasedPredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "content-based-predictions","content-based-predictions-user-%s.csv").toAbsolutePath.toString
  val finalPredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "final-predictions", "final-predictions-user-%s.csv").toAbsolutePath.toString
  val contentBasedModelForUserPath: String = Paths.get(datasetsDirectory, "model", "content-based", "user-%s").toAbsolutePath.toString
  val libsvmDirectoryPath: String = Paths.get(datasetsDirectory, "libsvm").toAbsolutePath.toString
  val collaborativePredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "collaborative-predictions").toAbsolutePath.toString
  val contentBasedPredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "content-based-predictions").toAbsolutePath.toString
  val finalPredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "final-predictions").toAbsolutePath.toString
}
