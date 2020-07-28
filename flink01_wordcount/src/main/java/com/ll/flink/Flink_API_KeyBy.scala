package com.ll.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink_API_KeyBy {
  def main(args: Array[String]): Unit = {
    //获取上下文流环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
//    env.setParallelism(2)

    //通过集合获取DataStream
    val collDS: DataStream[(String, Int)] = env.fromCollection(
      List(
        ("a", 1), ("b", 2), ("a", 3), ("b", 4)
      )
    )

    //通过key进行分组
    val transDS: KeyedStream[(String, Int), Tuple] = collDS.keyBy(0)

    //sink
    transDS.print("keyBy")

    //执行流
    env.execute()
  }
}
