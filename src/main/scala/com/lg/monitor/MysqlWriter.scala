package com.lg.monitor

import java.sql.{Connection, DriverManager}

import com.lg.bean.BusInfo
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.spark.sql.ForeachWriter

object MysqlWriter {
  val host="localhost"
  val port=3306
  val database="businfo"
  val username="root"
  val password="root"
  private val jdbcUrl =s"jdbc://mysql://$host:$port/$database??useUnicode=true&characterEncoding=utf-8"

  private val config = new HikariConfig()
  config.setJdbcUrl(jdbcUrl)
  config.setUsername(username)
  config.setPassword(password)
  config.addDataSourceProperty("cachePreStmts","true")
  config.addDataSourceProperty("preStmtCacheSize","250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit","2048")

  private val ds = new HikariDataSource(config)
  def getConnect = {
    //DriverManager.getConnection(jdbcUrl,username,password)
    ds.getConnection
  }
}

class MysqlWriter extends ForeachWriter[BusInfo] {
  var conn: Connection = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = MysqlWriter.getConnect
    true
  }

  override def process(value: BusInfo): Unit = {
    //把数据写入mysql
    val lglat: String = value.lglat
    val deployNum = value.deployNum
    val sql="insert into bus_trace(deploy_num,la_lat) values(?,?)"
    val ps = conn.prepareStatement(sql)
    ps.setString(1,deployNum)
    ps.setString(2,lglat)
    ps.executeUpdate()
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
