package gr.ml.analytics.service.contentbased

import org.apache.spark.ml.Pipeline

trait PipelineBuilder {

  def build(): Pipeline
}
