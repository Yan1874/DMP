package cn.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  val config = new JedisPoolConfig()
  config.setMaxTotal(20)
  config.setMaxIdle(10)

  val jedisPool = new JedisPool(config,"localhost",6379,10000)
  def getJedis() ={
    jedisPool.getResource
  }
}