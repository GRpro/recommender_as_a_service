package gr.ml.analytics.service

import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Interface to separate logic of feature extraction
  */
trait FeatureExtractor {

  def extract(rawItemDF: DataFrame, schema: Map[String, Any])
             (implicit sparkSession: SparkSession): Dataset[(Int, Vector)]

  def convertFeatures(rawItemDF: DataFrame, schema: Map[String, Any])
                     (implicit sparkSession: SparkSession): DataFrame =
    extract(rawItemDF, schema).toDF("itemid", "features")
}

class RowFeatureExtractor extends FeatureExtractor {


  // model the format of libsvm programmatically
  // see http://stackoverflow.com/questions/41416291/how-to-prepare-data-into-a-libsvm-format-from-dataframe

  override def extract(rawItemDF: DataFrame, schema: Map[String, Any])
                      (implicit sparkSession: SparkSession): Dataset[(Int, Vector)] = {

    import sparkSession.implicits._

    val allowedTypes = Set("double", "float")

    val idColumnName = schema("id").asInstanceOf[Map[String, String]]("name")
    val featureColumnNames = schema("features").asInstanceOf[List[Map[String, String]]]
      .filter((colDescription: Map[String, Any]) => allowedTypes.contains(colDescription("type").asInstanceOf[String].toLowerCase))
      .map(colDescription => colDescription("name"))

    val combinedItemsDF = rawItemDF.select(idColumnName, featureColumnNames:_*)

    val itemsDF = combinedItemsDF.map(row => {
      val id = row.getInt(0)
      val featureValues = (1 until row.length).map(idx => row.getDouble(idx))
      val featureValuesArr: Array[Double] = featureValues.toArray
      (id, Vectors.dense(featureValuesArr))
    })

    itemsDF
  }
}

/**
  * Feature extractor which scales every feature by multiplying it by a coefficient
  * received from map
  * @param ratioSupplier function with parameters (featureName) to return ratio for the feature
  */
class WeightedFeatureExtractor(ratioSupplier: String => Double) extends FeatureExtractor {

  val allowedFeatureTypes = Set("double", "float")

  override def extract(rawItemDF: DataFrame, schema: Map[String, Any])(implicit sparkSession: SparkSession): Dataset[(Int, Vector)] = {

    import sparkSession.implicits._

    val idColumnName = schema("id").asInstanceOf[Map[String, String]]("name")
    val featureColumnNames = schema("features").asInstanceOf[List[Map[String, String]]]
      .filter((colDescription: Map[String, Any]) => allowedFeatureTypes.contains(colDescription("type").asInstanceOf[String].toLowerCase))
      .map(colDescription => colDescription("name"))

    val combinedItemsDF = rawItemDF.select(idColumnName, featureColumnNames:_*)
    val featureRatios: Map[String, Double] = featureColumnNames.map(name => (name, ratioSupplier(name))).toMap

    val itemsDF = combinedItemsDF.map(row => {
      val id = row.getInt(0)

      val featureValues = featureColumnNames.map(name => {
        val ratio = featureRatios(name)
        val value = row.getAs[Double](name)
        value * ratio
      })

      val featureValuesArr: Array[Double] = featureValues.toArray
      (id, Vectors.dense(featureValuesArr))
    })

    itemsDF
  }
}