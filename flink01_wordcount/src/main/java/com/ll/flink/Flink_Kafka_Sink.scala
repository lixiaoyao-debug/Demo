package com.ll.flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object Flink_Kafka_Sink {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从文件中读取内容
    val fileDS: DataStream[String] = env.readTextFile("input/word.txt")

    //向kafka中输出数据
    val kafkaDSS: DataStreamSink[String] = fileDS.addSink(new FlinkKafkaProducer011[String]
    ("hadoop166:9092", "demo", new SimpleStringSchema()))

    //执行流
    env.execute()
  }
}
