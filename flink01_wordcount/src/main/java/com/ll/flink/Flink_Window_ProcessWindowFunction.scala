package com.ll.flink

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink_Window_ProcessWindowFunction {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //Source
    val sourceDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //transform
    val mapDS: DataStream[(String, Int)] = sourceDS.map((_, 1))
    val keyByKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    val processDS: DataStream[String] = keyByKS.timeWindow(Time.seconds(10)).process(new MyProcessWindowFunction)

    //打印
    processDS.print()

    //执行流
    env.execute()

  }
  class MyProcessWindowFunction extends ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
    override def process(key: String,
                         context: Context, elements:
                         Iterable[(String, Int)],
                         out: Collector[String]): Unit = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      out.collect("窗口启动时间："+sdf.format(new Date(context.window.getStart)))
      out.collect("窗口结束时间："+sdf.format(new Date(context.window.getEnd)))
      out.collect("计算的数据为："+elements.toList)
      out.collect("**********************************************")
    }
  }
}

