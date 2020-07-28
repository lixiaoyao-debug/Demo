package com.ll.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

object Flink_Window_Count {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //Source
    val socketDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //transform
    val mapDS: DataStream[(String, Int)] = socketDS.map((_, 1))

    //分流
    val keyByKS: KeyedStream[(String, Int), Tuple] = mapDS.keyBy(0)

    val countWS: WindowedStream[(String, Int), Tuple, GlobalWindow] = keyByKS.countWindow(3,2)

    //窗口计算时，根据keyBy后的数据的个数进行计算
    //当个数到达指定的值，那么会自动触发窗口数据的计算
    val reduceDS: DataStream[(String, Int)] = countWS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )

    //打印
    reduceDS.print()

    //执行流
    env.execute()
  }
}
