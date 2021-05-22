package com.lg.test

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 使用结构化流实现从socket读取数据实现单词统计
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1 获取sparksession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(WordCount.getClass.getName)
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //2 接收socket数据
    val df: DataFrame = spark.readStream
      .option("host", "node53")
      .option("port", 9999)
      .format("socket")
      .load()

    import spark.implicits._
    //3 处理数据，接收一行数据，按照空格进行切分
    //转为ds
    val ds: Dataset[String] = df.as[String]
    val wordDs: Dataset[String] = ds.flatMap(_.split(" "))
    // 4 使用dsl风格语句执行聚合统计
    val res: Dataset[Row] = wordDs.groupBy("value").count().sort($"count".desc)
    //输出
    res.writeStream
      .format("console") //输出到控制台
      .outputMode("complete") //指定输出模式，全部数据的计算结果
      .trigger(Trigger.ProcessingTime(0)) //尽可能快的触发计算
      .start() //启动
      .awaitTermination()

  }
}
