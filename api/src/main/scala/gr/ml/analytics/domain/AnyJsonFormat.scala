package gr.ml.analytics.domain

import spray.json.{JsFalse, JsNumber, JsString, JsTrue, JsValue, JsonFormat}

object AnyJsonFormat extends JsonFormat[Any] {
  def write(x: Any) = x match {
    case n: Int => JsNumber(n)
    case n: Double => JsNumber(n)
    case s: String => JsString(s)
    case b: Boolean if b => JsTrue
    case b: Boolean if !b => JsFalse
    case x => throw new RuntimeException("Do not understand object of type " + x.getClass.getName)
  }
  def read(value: JsValue) = value match {
    case JsNumber(n) => n.intValue()
    case JsString(s) => s
    case JsTrue => true
    case JsFalse => false
    case x => throw new RuntimeException("Do not understand how to deserialize " + x)
  }
}
