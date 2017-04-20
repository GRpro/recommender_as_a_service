package gr.ml.analytics.service.contentbased

import gr.ml.analytics.service.HybridServiceRunner.mainSubDir
import org.apache.spark.ml.Pipeline

object PipelineFactory {
  private val linearRegressionWithElasticNetPipeline = LinearRegressionWithElasticNetBuilder.build(mainSubDir)
  private val randomForestPipeline = RandomForestEstimatorBuilder.build(mainSubDir)
  private val generalizedLinearRegressionPipeline = GeneralizedLinearRegressionBuilder.build(mainSubDir)

  private val pipelines = List(
    linearRegressionWithElasticNetPipeline,
    randomForestPipeline,
    generalizedLinearRegressionPipeline)

  def getPipeline(id: Int): Pipeline ={
    pipelines(id)
  }

}
