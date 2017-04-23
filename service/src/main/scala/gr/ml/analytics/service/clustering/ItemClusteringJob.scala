package gr.ml.analytics.service.clustering

import com.typesafe.config.Config
import gr.ml.analytics.service.{Sink, Source}
import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable

class ItemClusteringJob(val source: Source,
                        val sink: Sink,
                        val config: Config)(implicit val sparkSession: SparkSession) {

  import sparkSession.implicits._

  def run(): Unit = {

    val dataset = source.getAllItemsAndFeatures().select($"itemid", $"features")

    // Trains a bisecting k-means model.
    val bkm = new BisectingKMeans().setK(50).setSeed(1)
    val model = bkm.fit(dataset)

    // Evaluate clustering.
    val cost = model.computeCost(dataset)
    println(s"Within Set Sum of Squared Errors = $cost")

    // Shows the result.
    println("Cluster Centers: ")
    val centers = model.clusterCenters
    centers.foreach(println)

    val result = model.transform(dataset)
    result.printSchema()

    val itemClustersDF = result
      .groupBy($"prediction")
      .agg(collect_set($"itemid").alias("itemids"))
      .select($"itemids")
      .flatMap(set => {
        val itemids: Set[Int] = set.getAs[mutable.WrappedArray[Int]]("itemids").toSet
        itemids.map(itemid => {
          val clusterElements = itemids - itemid
          (itemid, clusterElements.mkString(":"))
        })
      }).toDF("itemid", "similar_items")

    result.printSchema()
    result.show()

    sink.storeItemClusters(itemClustersDF)
  }
}


object ItemClusteringJob {

  def apply(source: Source, sink: Sink, config: Config) (implicit sparkSession: SparkSession): ItemClusteringJob =
    new ItemClusteringJob(source, sink, config)
}