package gr.ml.analytics.service

import java.nio.file.Paths

/**
  * Common constants used by service
  */
trait Constants {
  val smallDatasetFileName: String = "ml-latest-small.zip"
  val smallDatasetUrl: String = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

  val rootDataDir = "data"
  val mainSubDir = "main"
  val datasetsDirectory: String = Paths.get(rootDataDir, "%s").toAbsolutePath.toString
  val ratingsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings.csv").toAbsolutePath.toString
  val predictionsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "predictions.csv").toAbsolutePath.toString
  val moviesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies.csv").toAbsolutePath.toString
  val moviesWithFeaturesPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "movies-with-features.csv").toAbsolutePath.toString
  val ratingsWithFeaturesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "ratings-with-features-libsvm-user-%s.txt").toAbsolutePath.toString
  val allMoviesSVMPath: String = Paths.get(datasetsDirectory, "libsvm", "all-movies.txt").toAbsolutePath.toString
  val collaborativePredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "collaborative-predictions", "collaborative-predictions-user-%s.csv").toAbsolutePath.toString
  val contentBasedPredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "content-based-predictions","content-based-predictions-user-%s.csv").toAbsolutePath.toString
  val finalPredictionsForUserPath: String = Paths.get(datasetsDirectory, "predictions", "final-predictions", "final-predictions-user-%s.csv").toAbsolutePath.toString
  val collaborativeModelPath: String = Paths.get(datasetsDirectory, "model", "collaborative").toAbsolutePath.toString
  val contentBasedModelForUserPath: String = Paths.get(datasetsDirectory, "model", "content-based", "user-%s").toAbsolutePath.toString
  val libsvmDirectoryPath: String = Paths.get(datasetsDirectory, "libsvm").toAbsolutePath.toString
  val collaborativePredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "collaborative-predictions").toAbsolutePath.toString
  val contentBasedPredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "content-based-predictions").toAbsolutePath.toString
  val finalPredictionsDirectoryPath: String = Paths.get(datasetsDirectory, "predictions", "final-predictions").toAbsolutePath.toString
  val popularItemsPath: String = Paths.get(datasetsDirectory, "ml-latest-small", "popular-items.csv").toAbsolutePath.toString
  val estimationDirectoryPath: String = Paths.get(datasetsDirectory, "estimation").toAbsolutePath.toString
  val trainRatingsPath: String = Paths.get(datasetsDirectory, "estimation", "train-ratings.csv").toAbsolutePath.toString
  val testRatingsPath: String = Paths.get(datasetsDirectory, "estimation", "test-ratings.csv").toAbsolutePath.toString
  val trainRatingsPathSmall: String = Paths.get(datasetsDirectory, "estimation", "train-ratings-small.csv").toAbsolutePath.toString // TODO replace small with full version
  val testRatingsPathSmall: String = Paths.get(datasetsDirectory, "estimation", "test-ratings-small.csv").toAbsolutePath.toString // TODO replace small with full version
  val ratingsPathSmall: String = Paths.get(datasetsDirectory, "ml-latest-small", "ratings-small.csv").toAbsolutePath.toString
}
