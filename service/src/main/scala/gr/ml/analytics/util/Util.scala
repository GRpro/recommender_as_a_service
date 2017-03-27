package gr.ml.analytics.util

import java.io.FileOutputStream
import java.net.URL
import java.nio.file.{Files, Path}
import java.util.zip.ZipInputStream

import com.typesafe.scalalogging._

import scala.sys.process._

object Util extends LazyLogging {

  def loadResource(url: String, path: Path) = {
    if (!Files.exists(path)) {
      logger.info(s"$path does not exist. Creating.")
      path.getParent.toFile.mkdirs()
      path.toFile.createNewFile()
      val process: Process = new URL(url).#>(path.toFile).run()
      // wait until download is finished
      process.exitValue()
      logger.info(s"Resource $url was stored in $path")
    } else {
      logger.info(s"Resource $url already exists in $path")
    }
    path
  }

  def unzip(path: Path, destDir: Path): Unit = {
    val zis = new ZipInputStream(Files.newInputStream(path))

    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (!file.isDirectory) {
        val outPath = destDir.resolve(file.getName)
        val outPathParent = outPath.getParent
        if (!outPathParent.toFile.exists()) {
          outPathParent.toFile.mkdirs()
        }

        val outFile = outPath.toFile
        val out = new FileOutputStream(outFile)
        val buffer = new Array[Byte](4096)
        Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
      }
    }
  }

  //TODO remove this and all related stuff. This is temporary fix for Windows
  def windowsWorkAround(): Unit = {
    if (System.getProperty("os.name").contains("Windows")) {
      val HADOOP_BIN_PATH = getClass.getClassLoader.getResource("").getPath
      System.setProperty("hadoop.home.dir", HADOOP_BIN_PATH)
    }
  }
}
