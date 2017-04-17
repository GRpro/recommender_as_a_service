package gr.ml.analytics.util

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory

object ParamsStorage {

  val config = ConfigFactory.load("application.conf")

  private val host = config.getString("redis.host")
  private val port:Int = config.getInt("redis.port")
  private val r = new RedisClient(host, port)

  // TODO REMOVE it should be set in redis differently:
  r.hmset(config.getString("redis.cf_params_hash"), Map("rank" -> 10, "reg_param" -> 0.1))



  def getCFParams(): Map[String, Any] ={
    val rank = r.hmget(config.getString("redis.cf_params_hash"), "rank").get.get("rank")
    val reg_param = r.hmget(config.getString("redis.cf_params_hash"), "reg_param").get.get("reg_param")
    val map = Map("rank" -> rank.get, "reg_param" -> reg_param.get)
    map
  }

  def getCBParams(): Map[String, Any] ={
    Map() // TODO implement
  }
}
