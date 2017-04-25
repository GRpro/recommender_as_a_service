package gr.ml.analytics.util

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory

trait ParamsStorage {
  def getParams(): Map[String, Any]

  def getParam(key: String): Any

  def setParam(key: String, value: Any): Unit

}

class RedisParamsStorage extends ParamsStorage {

  val config = ConfigFactory.load("application.conf")

  private val host = config.getString("redis.host")
  private val port:Int = config.getInt("redis.port")
  private val r = new RedisClient(host, port)

  // TODO REMOVE it should be set in redis differently:
  r.hmset(config.getString("redis.ml_params_hash"), Map(
    "cf_rank" -> 10,
    "cf_reg_param" -> 0.2,

    "cb_pipeline_id" -> 0,

    "hb_collaborative_weight" -> 0.1,
    "hb_last_n_seconds" -> 24 * 3600
//     "hb_last_n_seconds" -> 2100000000// a big number close to integer upper range - for estimation service purposes
  ))

  // TODO REMOVE it should be set in redis differently:
  r.hmset(config.getString("redis.ml_params_hash"), Map(
    "schema.0.item.feature.f0.ratio" -> 2,
    "schema.0.item.feature.f1.ratio" -> 3,
    "schema.0.item.feature.f2.ratio" -> 6,
    "schema.0.item.feature.f3.ratio" -> 1,
    "schema.0.item.feature.f4.ratio" -> 3,
    "schema.0.item.feature.f5.ratio" -> 7,
    "schema.0.item.feature.f6.ratio" -> 3,
    "schema.0.item.feature.f7.ratio" -> 0.1,
    "schema.0.item.feature.f8.ratio" -> 3,
    "schema.0.item.feature.f9.ratio" -> 5,
    "schema.0.item.feature.f10.ratio" -> 4,
    "schema.0.item.feature.f11.ratio" -> 1,
    "schema.0.item.feature.f12.ratio" -> 1,
    "schema.0.item.feature.f13.ratio" -> 1,
    "schema.0.item.feature.f14.ratio" -> 4,
    "schema.0.item.feature.f15.ratio" -> 4,
    "schema.0.item.feature.f16.ratio" -> 1,
    "schema.0.item.feature.f17.ratio" -> 4,
    "schema.0.item.feature.f18.ratio" -> 1
  ))


  override def getParams(): Map[String, Any] = {
    val cfRank = r.hmget(config.getString("redis.ml_params_hash"), "cf_rank").get.get("cf_rank")
    val cfRegParam = r.hmget(config.getString("redis.ml_params_hash"), "cf_reg_param").get.get("cf_reg_param")

    val cbPipelineId = r.hmget(config.getString("redis.ml_params_hash"), "cb_pipeline_id").get.get("cb_pipeline_id")

    val hbCollaborativeWeight = r.hmget(config.getString("redis.ml_params_hash"), "hb_collaborative_weight").get.get("hb_collaborative_weight")
    val hbLastNSeconds = r.hmget(config.getString("redis.ml_params_hash"), "hb_last_n_seconds").get.get("hb_last_n_seconds")

    val map = Map(
      "cf_rank" -> cfRank.get,
      "cf_reg_param" -> cfRegParam.get,
      "cb_pipeline_id" -> cbPipelineId.get,
      "hb_collaborative_weight" -> hbCollaborativeWeight.get,
      "hb_last_n_seconds" -> hbLastNSeconds.get
    )
    map
  }

  override def getParam(key: String): Any = {
    r.hmget(config.getString("redis.ml_params_hash"), key).get(key)
  }

  override def setParam(key: String, value: Any): Unit = {
    r.hmset(config.getString("redis.ml_params_hash"), Map(
      key -> value
    ))
  }
}
