import java.nio.file.Paths

import data.Dataset

object App {


  def main(args: Array[String]): Unit = {


    val datasetsDirectory = "datasets"

    val smallDatasetFileName = "ml-latest-small.zip"
    val smallDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$smallDatasetFileName"

    val completeDatasetFileName = "ml-latest.zip"
    val completeDatasetUrl = s"http://files.grouplens.org/datasets/movielens/$completeDatasetFileName"

//    val smallDatasetPath = Dataset.loadResource(smallDatasetUrl, Paths.get(datasetsDirectory, smallDatasetFileName))
    val completeDatasetPath = Dataset.loadResource(completeDatasetUrl, Paths.get(datasetsDirectory, completeDatasetFileName))

//    Dataset.unzip(smallDatasetPath, Paths.get(smallDatasetPath.getParent.toString, smallDatasetPath.getFileName.toString.substring(0, smallDatasetPath.getFileName.toString.lastIndexOf("."))))
    Dataset.unzip(completeDatasetPath, Paths.get(datasetsDirectory))

  }
}
