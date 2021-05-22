package com.lg.monitor

import com.lg.bean.BusInfo
import org.apache.spark.sql.ForeachWriter
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisWriter {
  //
  private val config = new JedisPoolConfig
  //设置最大连接
  config.setMaxTotal(20)
  //设置空闲连接
  config.setMaxIdle(10)
  private val jedisPool = new JedisPool(config, "hadoop5", 6379, 1000)

  //从连接池中获取jedis对象

  def getConnection = {
    jedisPool.getResource
  }
}


/**
 * 写入redis的writer
 */
class RedisWriter extends ForeachWriter[BusInfo] {
  var jedis: Jedis = _

  //开启连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
    jedis = RedisWriter.getConnection
    true
  }

  //处理数据
  override def process(value: BusInfo): Unit = {
    //把数据写入redis ,kv形式
    val lglat: String = value.lglat
    val deployNum = value.deployNum
    jedis.set(deployNum, lglat)
  }

  //释放连接
  override def close(errorOrNull: Throwable): Unit = {
    jedis.close()
  }
}
