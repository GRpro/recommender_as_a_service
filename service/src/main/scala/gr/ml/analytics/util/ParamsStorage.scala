package gr.ml.analytics.util

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory

trait ParamsStorage{
  def getParams(): Map[String, Any]
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
  ))


  override def getParams(): Map[String, Any] ={
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
}
