package com.ll.flink

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink_API_CEP {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //数据源
    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.txt")

    //转换结构
    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(
          datas(0),
          datas(1).toLong,
          datas(2).toLong
        )
      }
    )

    //TODO
    //1.创建规则
    val pattern: Pattern[Sensor, Sensor] = Pattern
      //规则必须以begin开始，表示开始定义规则
      .begin[Sensor]("begin")
      .where(_.vc < 4)
      //增加新的条件，多个条件应该同时满足
      //.where(_.vc > 2)
      .or(_.vc > 8)

    //2.应用规则
    val sensorPS: PatternStream[Sensor] = CEP.pattern(sensorDS, pattern)

    //3.获取符合规则的结果
    val ds: DataStream[String] = sensorPS.select(
      map => {
        map.toString()
      }
    )

    //打印
    ds.print("cep>>>")

    //执行流
    env.execute()
  }
}
