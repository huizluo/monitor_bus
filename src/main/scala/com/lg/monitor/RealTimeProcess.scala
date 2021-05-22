package com.lg.monitor

import com.lg.bean.BusInfo
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

/**
  * 使用结构化流读取kafka中的数据
  */
object RealTimeProcess {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //1 获取sparksession
    val spark: SparkSession = SparkSession.builder()
      //      .master("local[*]")
      .appName(RealTimeProcess.getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    //2 定义读取kafka数据源
    val kafkaDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node52:9092,node53:9092")
      .option("subscribe", "lg_bus_info")
      .load()
    //3 处理数据
    val kafkaValDf: DataFrame = kafkaDf.selectExpr("CAST(value AS STRING)")
    //转为ds
    val kafkaDs: Dataset[String] = kafkaValDf.as[String]
    //解析出经纬度数据，写入redis
    //封装为一个case class方便后续获取指定字段的数据
    val busInfoDs: Dataset[BusInfo] = kafkaDs.map(BusInfo(_)).filter(_ != null)
    //把经纬度数据写入redis
    busInfoDs.writeStream
      .foreach(new MysqlWriter)
      .outputMode("append")
      .start()
    //          .awaitTermination()
    //把经纬度数据写入Hbase
    busInfoDs.writeStream
      .foreach(
        new HbaseWriter
      )
      .outputMode("append")
      .start()
    //              .awaitTermination()
    //实现对车辆异常情况的监测
    val warnInfoDs = busInfoDs.filter(
      info => {
        val remain: String = info.oilRemain
        remain.toInt < 30 //剩余油量小于30%
      }
    )
    //写入到kafka另外一个主题，由web系统监听，然后推送警告信息到车载客户单
    //写出的ds/df中必须有一个列名叫做value
    warnInfoDs.withColumn("value", new Column("deployNum"))
      .writeStream
      .format("kafka")
      .option("checkpointLocation", "hdfs://lgns/realtime") //ck目录一般选择是hdfs目录
      .option("kafka.bootstrap.servers", "node52:9092,node53:9092")
      .option("topic", "lg_bus_warn_info")
      .start()
    //      .awaitTermination()

    spark.streams.awaitAnyTermination()
  }
}
