package com.ll.flink


import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink_Time_WaterMark {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //设置事件时间为
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //测试
    //监控某端口
    val dataDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)
    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toInt)
      }
    )

    //抽取时间戳和设定水位线(水印)
    //1.从数据中抽取数据作为时间时间
    //2.设定水位线标记，这个标记一般比当前数据事件时间要推迟
    val markDS: DataStream[Sensor] = sensorDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(3)) {
        //抽取事件时间，以毫秒为单位
        override def extractTimestamp(t: Sensor): Long = {
          t.ts * 1000L
        }
      }
    )


    val applyDS: DataStream[String] = markDS
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(
        // 对窗口进行数据处理
        // key : 分流的key
        // window : 当前使用窗口的类型
        // datas : 窗口中的数据
        // out : 输出,指定输出的类型
        (key: String, window: TimeWindow, datas: Iterable[Sensor], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val start = window.getStart
          val end = window.getEnd
          out.collect(s"[${start}-${end}), 数据[${datas}]")
        }
      )


    //打印
    markDS.print("mark>>>")
    applyDS.print("window>>>")

    //执行流
    env.execute()
  }
}

