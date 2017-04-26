package gr.ml.analytics.domain

import spray.json.DefaultJsonProtocol._
import spray.json.{JsArray, JsFalse, JsNumber, JsObject, JsString, JsTrue, JsValue, JsonFormat, RootJsonFormat}

object JsonSerDeImplicits {
  implicit val ratingFormat: RootJsonFormat[Rating] = jsonFormat4(Rating.apply)

  implicit val schemaFormat: JsonFormat[Schema] = jsonFormat2(Schema.apply)

  implicit val anyFormat: JsonFormat[Any] = AnyJsonFormat



  implicit object AnyJsonFormat extends JsonFormat[Any] {
    def write(x: Any) = x match {
      case n: Int => JsNumber(n)
      case s: String => JsString(s)
      case x: Seq[_] => seqFormat[Any].write(x)
      case m: Map[String, _] => mapFormat[String, Any].write(m)
      case b: Boolean if b == true => JsTrue
      case b: Boolean if b == false => JsFalse
      case x => throw new RuntimeException
    }
    def read(value: JsValue) = value match {
      case JsNumber(n) => n.intValue()
      case JsString(s) => s
      case a: JsArray => listFormat[Any].read(value)
      case o: JsObject => mapFormat[String, Any].read(value)
      case JsTrue => true
      case JsFalse => false
      case x => throw new RuntimeException
    }
  }
}
