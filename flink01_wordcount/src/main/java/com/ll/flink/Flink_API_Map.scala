package com.ll.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.e


object Flink_API_Map {
  def main(args: Array[String]): Unit = {
    //构建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行数
    env.setParallelism(2)

    //创建DataStream
    val collDS: DataStream[Int] = env.fromCollection(
      List(1, 2, 3, 4)
    )

    //进行map和flatmap的转换
    val elemDS: DataStream[Int] = collDS.map(elem => elem * 2)
    val flatDS: DataStream[Int] = collDS.flatMap(num => List(num))
    val filterDS: DataStream[Int] = collDS.filter(num => num % 2 == 0)


    //sink
    elemDS.print("map")
    flatDS.print("flatMap")
    filterDS.print("filter")

    //执行流
    env.execute("Demo")
  }
}
