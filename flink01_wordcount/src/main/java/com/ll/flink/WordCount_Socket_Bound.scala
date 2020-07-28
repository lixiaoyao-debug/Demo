package com.ll.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object WordCount_Socket_Bound {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建Flink的上下文执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //改变并行度
    env.setParallelism(2)

    //TODO 2.获取数据
    val lineDS: DataStream[String] = env.socketTextStream("hadoop166",9999)
    
    //3.扁平化每行读到的数据
    val wordDS: DataStream[String] = lineDS.flatMap(line => line.split(" "))

    //4.转换wordDS的结构
    val mapDS: DataStream[(String, Int)] = wordDS.map((_, 1))
    val groDS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    //5.对转换的TransDS进行累加
    val sumDS: DataStream[(String, Int)] = groDS.sum(1)

    //6.打印
    sumDS.print()

    //7.Flink是一个流数据处理框架，而且是一个事件驱动的框架
    //让Flink流处理运行起来
    env.execute()
  }
}
