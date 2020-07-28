package com.ll.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{StreamTableEnvironment,_}

object Flink_API_Table {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    //TODO 使用Table API

    //获取TableAPI环境
    val tableEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //将数据转换为一张表
    val table: Table = tableEnv.fromDataStream(sensorDS)

    //使用SQL操作数据
    val result: DataStream[(String, Long, Double)] = tableEnv.sqlQuery("select * from " + table).toAppendStream[(String, Long, Double)]

    //打印
    result.print("table>>>")

    //执行流
    env.execute()
  }
}
