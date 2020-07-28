package com.ll.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object Flink_Window_MyAggregateFunction {
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
    val keyByKS: KeyedStream[(String, Int), String] = mapDS.keyBy(_._1)

    val myAgreeDS: DataStream[Int] = keyByKS.timeWindow(Time.seconds(3)).aggregate(
      new MyAggregateFunction
    )

    //打印
    myAgreeDS.print("aggregate>>>")

    //执行流
    env.execute()
  }

  class MyAggregateFunction extends AggregateFunction[(String, Int), Int, Int] {
    override def createAccumulator(): Int = 0

    override def add(in: (String, Int), acc: Int): Int = {
      println("acc...")
      acc + in._2
    }

    override def getResult(acc: Int): Int = {
      acc
    }

    override def merge(acc: Int, acc1: Int): Int = {
      acc + acc1
    }
  }

}
