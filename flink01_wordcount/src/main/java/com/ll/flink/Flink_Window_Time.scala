package com.ll.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

object Flink_Window_Time {
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
    val socketKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    //TODO 时间窗口：以时间作为数据的处理范围
    //如果多个窗口之间不重合，并且头结尾，尾接头，称之为滚动窗口
    val socketWS: WindowedStream[(String, Int), String, TimeWindow] = socketKS.timeWindow(
      Time.seconds(3),
      Time.seconds(2)
    )

    val reduceDS: DataStream[(String, Int)] = socketWS.reduce(
      (t1, t2) => {
        (t1._1, t1._2 + t2._2)
      }
    )

    //打印
    reduceDS.print("window>>>")

    //执行流
    env.execute()
  }
}
