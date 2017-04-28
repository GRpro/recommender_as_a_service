package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

@ApiModel(description = "Schema object")
case class Schema(
                   @(ApiModelProperty @field)(value = "unique identifier for the schema")
                   schemaId: Int,
                   @(ApiModelProperty @field)(value = "json definition of the schema")
                   jsonSchema: Map[String, Any])

object Schema