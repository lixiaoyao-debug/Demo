package com.ll.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object WordCount_Para_Demo {
  def main(args: Array[String]): Unit = {

    //1.创建Flink上下文执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.获取数据
    val lineDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //3.扁平化数据
    val splitDS: DataStream[String] = lineDS.flatMap(_.split(" "))

    //3.转换数据结构
    val mapToOneDS: DataStream[(String, Int)] = splitDS.map((_, 1))

    //4.对相同key数据进行集合
    val keyByDS: KeyedStream[(String, Int), Tuple] = mapToOneDS.keyBy(0)

    //4.对同key数据进行累加
    val sumDS: DataStream[(String, Int)] = keyByDS.sum(1)

    //5.打印
    sumDS.print()

    //6.执行Flink流处理
    env.execute()

  }
}
