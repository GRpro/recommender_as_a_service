import java.nio.file.Paths

import data.Dataset

object App {


  def main(args: Array[String]): Unit = {

    Dataset.loadResource(Dataset.smallDatasetUrl, Paths.get(Dataset.datasetsDirectory, Dataset.smallDatasetFileName))
    Dataset.loadResource(Dataset.completeDatasetUrl, Paths.get(Dataset.datasetsDirectory, Dataset.completeDatasetFileName))
  }
}
