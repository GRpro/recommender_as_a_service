package gr.ml.analytics.domain

import spray.json.DefaultJsonProtocol._
import spray.json.{JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat}

object JsonSerDeImplicits {
  implicit val ratingFormat: RootJsonFormat[Rating] = jsonFormat4(Rating.apply)

  implicit val anyFormat: JsonFormat[Any] = AnyJsonFormat

  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case n: Double => JsNumber(n)
      case s: String => JsString(s)
      case b: Boolean if b => JsTrue
      case b: Boolean if !b => JsFalse
    }
    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case JsTrue => true
      case JsFalse => false
    }
  }

  implicit object ItemJsonFormat extends RootJsonFormat[Item] {

    def write(c: Item) = mapFormat[String, Any].write(c)

    def read(value: JsValue) = value match {
      case x: JsObject => x.fields match {
        case x: Map[String, JsValue] => x transform {
          case (a: String, b: JsObject) => read(b)
          case (a: String, JsString(s)) => s
          case (a: String, JsNumber(n)) => n
          case (a: String, _) => null
        }
      }
      case _ => null
    }
  }
}
