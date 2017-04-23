package com.gr.ml.analytics.demo.extractor

import java.io.File

import com.gr.ml.analytics.demo.{Constants, Util}
import org.slf4j.LoggerFactory

/**
  * Run this class to extract movies genre features in a numeric format and
  * write to file.
  */
object MovielensFeaturesExtractor extends Constants {

  val subRootDir = "preprocessing"

  def prepareNecessaryFiles(): Unit = {
    val startTime = System.currentTimeMillis()

    if (!new File(String.format(moviesWithFeaturesPath, subRootDir)).exists()) {
      new GenresFeatureEngineering(subRootDir).createAllMoviesWithFeaturesFile()
    }

    val finishTime = System.currentTimeMillis()

    LoggerFactory.getLogger("progressLogger").info(subRootDir + " :: Startup time took: " + (finishTime - startTime) + " millis.")
  }

  def main(args: Array[String]): Unit = {
    Util.loadAndUnzip()
    prepareNecessaryFiles()
  }
}
