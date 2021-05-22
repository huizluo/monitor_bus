### 智慧物流项目作业
 使用HikariCP管理mysql数据库连接池
 
 路径:com.log.monitor.MysqlWriter
 
 ```
class MysqlWriter extends ForeachWriter[BusInfo] {
  var conn: Connection = _

  // open时从mysql连接池拿去连接
  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = MysqlWriter.getConnect
    true
  }

 //process 执行插入sql
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

 //关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
 ```