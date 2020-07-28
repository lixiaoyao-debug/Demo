package com.ll.flink

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object Flink_Transform_Function {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val sourceDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    //transform
    //类型转换使用map
    val myMapDS: DataStream[Sensor] = sourceDS.map(new MyMapFunction)

    //打印
    myMapDS.print()

    //执行流
    env.execute()

  }
}

class MyMapFunction extends MapFunction[String,Sensor]{
  override def map(t: String): Sensor = {
    val datas: Array[String] = t.split(",")
    Sensor(datas(0),datas(1).toLong,datas(2).toDouble)
  }
}