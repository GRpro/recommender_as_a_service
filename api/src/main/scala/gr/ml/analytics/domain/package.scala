package gr.ml.analytics

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

package object domain {

  @ApiModel(description = "An item object")
  type Item = Map[String, Any]

  @ApiModel(description = "Rating object")
  case class Rating(
                     @(ApiModelProperty @field)(value = "unique identifier for the user")
                     userId: Int,
                     @(ApiModelProperty @field)(value = "unique identifier for the item")
                     itemId: Int,
                     @(ApiModelProperty @field)(value = "rating to describe how user prefer an item")
                     rating: Double,
                     @(ApiModelProperty @field)(value = "long timestamp when user rated item")
                     timestamp: Long)

  object Rating

  @ApiModel(description = "Schema object")
  case class Schema(
                     @(ApiModelProperty @field)(value = "unique identifier for the schema")
                     schemaId: Int,
                     @(ApiModelProperty @field)(value = "json definition of the schema")
                     jsonSchema: Map[String, Any])

  object Schema

  @ApiModel(description = "Recommendation object")
  case class Recommendation(
                             @(ApiModelProperty @field)(value = "unique identifier of the user")
                             userId: Int,
                             @(ApiModelProperty @field)(value = "sorted list of recommended items represented by sorted list of item ids")
                             topItems: List[Int])

  object Recommendation

  case class ClusteredItems(itemId: Int, similarItems: Set[Int])

  object ClusteredItems
}
