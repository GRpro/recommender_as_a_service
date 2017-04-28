package gr.ml.analytics

import io.swagger.annotations.{ApiModel, ApiModelProperty}

import scala.annotation.meta.field

package object domain {

  @ApiModel(description = "An item object")
  type Item = Map[String, Any]






  case class ClusteredItems(itemId: Int, similarItems: Set[Int])

  object ClusteredItems
}
