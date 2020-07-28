package com.ll.flink

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object Flink_Transform_Aggregate {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //Source：从集合中获取数据
    val sourceDS: DataStream[(String, Int)] = env.fromCollection(
      List(
        ("a", 1), ("b", 2), ("a", 3), ("b", 4)
      )
    )

    //想要进行aggregate转换，需先对数据进行keyBy
    val ksDS: KeyedStream[(String, Int), Tuple] = sourceDS.keyBy(0)

    //进行滚动聚合操作
    //    val aggreDS: DataStream[(String, Int)] = ksDS.sum(1)
//    val aggreDS: DataStream[(String, Int)] = ksDS.min(1)
//    val aggreDS: DataStream[(String, Int)] = ksDS.max(1)
    val aggreDS: DataStream[(String, Int)] = ksDS.reduce(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )


    //打印
    aggreDS.print("sum>>>")

    //执行
    env.execute()
  }
}
