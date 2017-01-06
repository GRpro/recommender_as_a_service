package data

import sys.process._
import java.net.URL
import java.nio.file.{Files, Path}

import com.typesafe.scalalogging._

object Dataset extends LazyLogging {

  val datasetsDirectory = "datasets"

  val smallDatasetFileName = "ml-latest-small.zip"
  val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

  val completeDatasetFileName = "ml-latest.zip"
  val completeDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$completeDatasetFileName"

  def loadResource(url: String, path: Path) = {
    if (!Files.exists(path)) {
      logger.info(s"$path does not exist. Creating.")
      path.getParent.toFile.mkdirs()
      path.toFile.createNewFile()
      (new URL(smallDatasetUrl) #> path.toFile !!)
      logger.info(s"Resource $url was stored in $path")
    } else {
      logger.info(s"Resource $url already exists in $path")
    }
  }
}
