package com.ll.flink.common

import java.util.Properties

import com.ll.flink.util.FlinkStreamEnv
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * 通用数据访问特质
 */
trait TDao {

  //从文件中读取数据
  def readFile()={
    FlinkStreamEnv.get().readTextFile(path)
  }

  //从kafka读取数据
  def readKafka()={
    //kafka相关配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop166:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //从kafka中获取数据
    val kafkaDS: DataStream[String] = FlinkStreamEnv.get().addSource(
      new FlinkKafkaConsumer011[String](
        "sensor",
        new SimpleStringSchema(),
        properties
      )
    )
    kafkaDS
  }

  //从socket读取网络数据
  def readSocket()={

  }
}
