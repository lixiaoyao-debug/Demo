package com.ll.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Flink_Time_Event {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //默认情况下，flink处理数据采用的时间为Processing Time
    //但是实际业务中一般都采用EventTime
    //可以通过环境对象设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //执行流
    env.execute()
  }
}
