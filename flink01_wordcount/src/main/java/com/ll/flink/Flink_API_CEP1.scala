package com.ll.flink

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}

object Flink_API_CEP1 {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.txt")

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
      .where(_.id=="sensor")
    //2.应用规则
    val sensorPS: PatternStream[Sensor] = CEP.pattern(sensorDS, pattern)

    //3.获取符合规则的结果
    val ds: DataStream[String] = sensorPS.select(
      map => {
        map.toString()
      }
    )

    ds.print("cep>>>")

    env.execute()
  }
}
