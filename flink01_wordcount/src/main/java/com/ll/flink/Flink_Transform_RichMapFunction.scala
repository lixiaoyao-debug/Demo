package com.ll.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

object Flink_Transform_RichMapFunction {
  def main(args: Array[String]): Unit = {
    //获取上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val sourceDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    //转换
    val richDS: DataStream[Sensor] = sourceDS.map(new MyRichMapFunction)

    //打印
    richDS.print()

    //执行流
    env.execute()
  }
}

class MyRichMapFunction extends RichMapFunction[String,Sensor ]{
  override def map(in: String): Sensor = {
    val datas: Array[String] = in.split(",")
    Sensor(datas(0),datas(1).toLong,datas(2).toDouble)
  }
}