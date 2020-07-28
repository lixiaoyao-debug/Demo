package com.ll.flink

import org.apache.flink.api.scala._

object WordCount_Batch {
  def main(args: Array[String]): Unit = {
    //TODO 1.构建Flink运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //TODO 2.获取数据
    val lineDS: DataSet[String] = env.readTextFile("input")
    
    //TODO 3.扁平化：将整体拆分成个体
    val wordDS: DataSet[String] = lineDS.flatMap(line => line.split(" "))

    //4.转换wordDS的结构
    val mapDS: DataSet[(String, Int)] = wordDS.map((_, 1))

    //TODO 将转换结构后的数据按照单词进行分组，聚合数据
    val groDS: GroupedDataSet[(String, Int)] = mapDS.groupBy(0)

    //TODO 5.对转换的TransDS进行累加
    val sumDS: AggregateDataSet[(String, Int)] = groDS.sum(1)

    //6.打印
    sumDS.print()

  }
}
