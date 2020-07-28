package com.ll.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object FlinkSourceKafka {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //kafka中的配置文件
    /*val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop166:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")*/

//    //连接kafka
//    val kafkaDS: DataStream[String] = env
//      .addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

//    val mySensorDS: DataStream[Sensor] = env.addSource(new MySensorSource)

  //从文件中读
//  val sensorDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    //从集群中读
    val sensorDS: DataStream[(String, Int, Double)] = env.fromCollection(List(
      ("ws_001", 1577844001, 45.0),
      ("ws_002", 1577844015, 43.0),
      ("ws_003", 1577844020, 42.0)
    ))

    val mapDS: DataStream[(String, String, String)] = sensorDS.map(
      t => {
        ("id:" + t._1, "ts:" + t._2, "vc:" + t._3)
      }
    )

    //Sink
    mapDS.print()

    //执行流
    env.execute("WaterSensor")
  }
}
