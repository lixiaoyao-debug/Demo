package com.ll.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink_Time_Watermark2 {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //设置时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //改变水位线数据生成的周期
    env.getConfig.setAutoWatermarkInterval(5000)

    //Source
    val dataDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)
    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    val markDS: DataStream[Sensor] = sensorDS.assignTimestampsAndWatermarks(
      //自定义事件时间的抽取和生成水位线watermark
      new AssignerWithPunctuatedWatermarks[Sensor] {
        override def checkAndGetNextWatermark(lastElement: Sensor, extractedTimestamp: Long): Watermark = {
          println("checkAndGetNextWatermark...")
          //间歇性生成水位线数据
          new Watermark(extractedTimestamp)
        }
        override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = {
          //抽取事件时间
          element.ts * 1000
        }
      }
    )

    val applyDS: DataStream[String] = markDS.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .apply(
        (key: String, window: TimeWindow, datas: Iterable[Sensor], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val start: Long = window.getStart
          val end: Long = window.getEnd
          out.collect(s"[$start-$end]，数据[$datas]")
        }
      )

    markDS.print("mark>>>")
    //计算正常数据的窗口
    applyDS.print("window>>>")

    //执行流
    env.execute()
  }
}
