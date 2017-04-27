package gr.ml.analytics.domain

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field


@ApiModel(description = "Recommendation object")
case class Recommendation(
                           @(ApiModelProperty @field)(value = "unique identifier of the user")
                           userId: Int,
                           @(ApiModelProperty @field)(value = "sorted list of recommended items represented by sorted list of item ids")
                           topItems: List[Int])

object Recommendation