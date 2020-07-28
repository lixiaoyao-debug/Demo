package com.ll.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FlinkReadFromLinux {
  def main(args: Array[String]): Unit = {
    //创建上下文运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从linux系统中的读取文件
    val sourceDS: DataStream[String] = env.readTextFile("hdfs://hadoop166:9000/warehouse/gmall/ads/ads_back_count")

    //打印
    sourceDS.print("linuxFile>>>")


    //执行流
    env.execute()
  }
}
