package com.lg.monitor

import com.lg.bean.BusInfo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.ForeachWriter

object HbaseWriter {
  //hbase中的connection本身底层已经使用了线程池，而且connection是线程安全的，可以全局使用一个，
  //但是对admin,table需要每个线程使用一个

  def getHtable(): Table = {
    //获取连接
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "hadoop3,hadoop4")
    val conn: Connection = ConnectionFactory.createConnection(conf)
    //hbase表名：htb_gps
    val table: Table = conn.getTable(TableName.valueOf("htb_gps"))
    table
  }
}


class HbaseWriter extends ForeachWriter[BusInfo] {
  var table: Table = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    table = HbaseWriter.getHtable()
    true
  }

  override def process(value: BusInfo): Unit = {
    //rowkey:调度编号+车牌号+时间戳
    var rowkey = value.deployNum + value.plateNum + value.timeStr
    val put = new Put(Bytes.toBytes(rowkey))
    val arr: Array[String] = value.lglat.split("_")
    //经度
    put.addColumn(
      Bytes.toBytes("car_info"),
      Bytes.toBytes("lng"),
      Bytes.toBytes(arr(0))
    )
    //维度
    put.addColumn(
      Bytes.toBytes("car_info"),
      Bytes.toBytes("lat"),
      Bytes.toBytes(arr(1))
    )
    table.put(put)
  }

  override def close(errorOrNull: Throwable): Unit = {
    table.close()
  }
}
