package com.lg.bean

//接收各个字段
case class BusInfo(
                    deployNum: String,
                    simNum: String,
                    transportNum: String,
                    plateNum: String,
                    lglat: String,
                    speed: String,
                    direction: String,
                    mileage: String,
                    timeStr: String,
                    oilRemain: String,
                    weights: String,
                    acc: String,
                    locate: String,
                    oilWay: String,
                    electric: String
                  )

object BusInfo {

  def apply(
             msg: String
           ): BusInfo = {
    //获取一条消息，按照逗号切分，准备各个字段数据然后获取businfo对象
    val arr: Array[String] = msg.split(",")
    if (arr.length == 15) {
      BusInfo(
        arr(0),
        arr(1),
        arr(2),
        arr(3),
        arr(4),
        arr(5),
        arr(6),
        arr(7),
        arr(8),
        arr(9),
        arr(10),
        arr(11),
        arr(12),
        arr(13),
        arr(14)
      )
    }else{
      null
    }


  }
}
