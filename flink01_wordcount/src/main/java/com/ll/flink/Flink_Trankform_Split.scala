package com.ll.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.e

object Flink_Trankform_Split {
  def main(args: Array[String]): Unit = {
    //获取上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val sourceDS: DataStream[(String, Int)] = env.fromCollection(
      List(
        ("abc", 1), ("bbc", 2), ("acb", 3), ("bcb", 4), ("aba", 5), ("aab", 6)
      )
    )

    //将数据根据条件进行分流
    val dataSS: SplitStream[(String, Int)] = sourceDS.split(
      t => {
        val key: String = t._1
        val splitKey: String = key.substring(1, 2)
        if (splitKey == "a") {
          Seq("a", "aa")
        } else if (splitKey == "b") {
          Seq("b", "bb")
        } else {
          Seq("c", "cc")
        }
      }
    )

    val ads: DataStream[(String, Int)] = dataSS.select("a")
    val bds: DataStream[(String, Int)] = dataSS.select("b")
    val cds: DataStream[(String, Int)] = dataSS.select("c")
    val cbds: DataStream[(String, Int)] = dataSS.select("c", "b")

    //打印
    ads.print("aaa>>>")
    bds.print("bbb>>>")
//    cds.print("ccc>>>")
//    cbds.print("cbcbcb>>>")

    val abDS: ConnectedStreams[(String, Int), (String, Int)] = ads.connect(bds)

    val mapDS: DataStream[(String, Int)] = abDS.map(
      t1 => t1,
      t2 => t2
    )

    mapDS.print()
    //流执行
    env.execute()
  }
}
