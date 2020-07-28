package com.ll.flink

import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object Flink_ES_Sink {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据
    val dataDS: DataStream[String] = env.readTextFile("input/sensor.txt")

    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("hadoop166", 9200))

    val ds: DataStream[Sensor] = dataDS.map(
      s => {
        val datas = s.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )
    val esSinkBuilder = new ElasticsearchSink.Builder[Sensor](httpHosts, new ElasticsearchSinkFunction[Sensor] {
      override def process(t: Sensor, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        println("saving data: " + t)
        val json = new util.HashMap[String, String]()
        json.put("data", t.toString)
        val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(json)
        requestIndexer.add(indexRequest)
        println("saved successfully")
      }
    })
    // 启动ES时。请使用es用户
    // 访问路径：http://hadoop01:9200/_cat/indices?v
    // 访问路径：http://hadoop01:9200/sensor/_search
    ds.addSink(esSinkBuilder.build())

    //执行流
    env.execute()

  }
}
