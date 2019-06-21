package com.sun.bigdata.sparkmall.common.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * title: RedisUtil 
  * projectName sparkmall 
  * description: 针对Redis的工具类
  * author Sun-Smile 
  * create 2019-06-21 14:36 
  */
object RedisUtil {
  var jedisPool: JedisPool = null

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val host = ConfigUtils.getValueConfig("redis.host")
      val port = ConfigUtils.getValueConfig("redis.port").toInt

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port)
    }
    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    println("获得一个连接")
    jedisPool.getResource
  }

}
