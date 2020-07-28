package com.ll.flink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object Flink_MySQL_Sink {
  def main(args: Array[String]): Unit = {
    //获取上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //source
    val dataDS: DataStream[String] = env.fromCollection(
      List("a", "b", "c")
    )

    //Sink
    dataDS.addSink(new MySQLSink)

    //执行流
    env.execute()
  }
}

//自定义Sink
class MySQLSink extends RichSinkFunction[String]{
  var conn:Connection=null
  var pstat:PreparedStatement=null

  //建立连接
  override def open(parameters: Configuration): Unit = {
    conn=DriverManager.getConnection("jdbc:mysql://hadoop166:3306/test",
      "root",
      "666666")
    pstat=conn.prepareStatement(
      "insert into user (id,name,age) values (?,?,?)")
  }

  override def close(): Unit = {
    if(pstat!=null){
      pstat.close()
    }
    if(conn!=null){
      conn.close()
    }
  }

  //释放连接
  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    pstat.setInt(1,1)
    pstat.setString(2,value)
    pstat.setInt(3,10)
    pstat.execute()
  }
}