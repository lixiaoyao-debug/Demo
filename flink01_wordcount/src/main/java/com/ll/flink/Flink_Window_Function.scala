package com.ll.flink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink_Window_Function {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //Source
    val sourceDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //transform
    val mapDS: DataStream[(String, Int)] = sourceDS.map((_, 1))
    //分流
    val keyByKS: KeyedStream[(String, Int), Tuple] = mapDS.keyBy(0)

    val myReduceDS: DataStream[(String, Int)] = keyByKS.timeWindow(Time.seconds(3)).reduce(
      new MyReduceFunction
    )

    //打印
    myReduceDS.print()

    //执行流
    env.execute()
  }

  class MyReduceFunction extends ReduceFunction[(String, Int)] {
    override def reduce(t: (String, Int), t1: (String, Int)): (String, Int) = {
      println("reduce=" + t._2 + "+" + t1._2)
      (t._1, t._2 + t1._2)
    }
  }
}
