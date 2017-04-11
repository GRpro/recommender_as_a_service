package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.contentbased.RandomForestEstimatorBuilder.allMoviesSVMPath
import gr.ml.analytics.util.SparkUtil
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{DecisionTreeRegressor, GeneralizedLinearRegression}

// TODO this one fails - assertion failed: lapack.dppsv returned 7.
object DecisionTreeRegressionBuilder {

  // TODO userId is NOT actually used...
  def build(subRootDir: String): Pipeline = {
    val spark = SparkUtil.sparkSession()
    // Load and parse the data file, converting it to a DataFrame.
    val data = spark.read.format("libsvm")
      .load(String.format(allMoviesSVMPath, subRootDir))

    val glr = new GeneralizedLinearRegression()
      .setFamily("gaussian")
      .setLink("identity")
      .setMaxIter(10)
      .setRegParam(0.3)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    spark.stop()

    pipeline
  }
}
