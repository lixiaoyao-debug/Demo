package com.ll.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.map

object Flink_Transform_Connect {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //source
    val dataDS1: DataStream[Int] = env.fromCollection(List(1, 2))
    val dataDS2: DataStream[String] = env.fromCollection(List("a", "b"))
    val dataDS3: DataStream[String] = env.fromCollection(List("c", "d"))

    //将两个DS进行Connect
    /*
        val connDS: ConnectedStreams[Int, String] = dataDS1.connect(dataDS2)
        val mapDS: DataStream[Any] = connDS.map(
          num => num,
          t => t
        )
    */

    //union合并两个流，这两个流的类型必须一致
    val unionDS: DataStream[String] = dataDS1.map(_.toString).union(dataDS2,dataDS3)

    //打印
    unionDS.print()

    //执行流
    env.execute()
  }
}
