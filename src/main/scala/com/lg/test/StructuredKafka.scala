package com.lg.test

/**
  * 使用结构化流读取kafka中的数据
  */
object StructuredKafka {
  def main(args: Array[String]): Unit = {
    //1 获取sparksession
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(StructuredKafka.getClass.getName)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    //2 定义读取kafka数据源
    val kafkaDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "hadoop4:9092")
      .option("subscribe", "spark_kafka")
      .load()
    //3 处理数据
    val kafkaValDf: DataFrame = kafkaDf.selectExpr("CAST(value AS STRING)")
    //转为ds
    val kafkaDs: Dataset[String] = kafkaValDf.as[String]
    val kafkaWordDs: Dataset[String] = kafkaDs.flatMap(_.split(" "))
    //执行聚合
    val res: Dataset[Row] = kafkaWordDs.groupBy("value").count().sort($"count".desc)
    //4 输出
    res.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
}
