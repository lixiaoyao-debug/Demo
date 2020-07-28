package com.ll.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.util.Collector

object Flink_API_Req {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    env.setParallelism(1)

    //设置时间为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Source
    val dataDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //transform
    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    //    val wsDS: DataStream[Sensor] = sensorDS.assignAscendingTimestamps(_.ts * 1000)
    // 如果采用assignAscendingTimestamps方式抽取事件时间
    // 那么watermark = EventTime - 1ms
    val wsDS: DataStream[Sensor] = sensorDS.assignTimestampsAndWatermarks(
      new AssignerWithPunctuatedWatermarks[Sensor] {
        override def checkAndGetNextWatermark(lastElement: Sensor,
                                              extractedTimestamp: Long): Watermark = {
          new Watermark(extractedTimestamp)
        }

        override def extractTimestamp(element: Sensor, previousElementTimestamp: Long): Long = {
          element.ts * 1000L
        }
      }
    )

    val sensorKS: KeyedStream[Sensor, String] = wsDS.keyBy(_.id)

    //TODO 监控水位传感器的水位值
    //如果水位值在五秒钟之内连续上升，则报警
    val processDS: DataStream[String] = sensorKS.process(
      new KeyedProcessFunction[String, Sensor, String] {
        //当前水位值数据
        private var currentHeight = 0L
        //定时器
        private var alarmTimer = 0L

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor, String]#OnTimerContext, out: Collector[String]): Unit = {
          out.collect("水位传感器【" + ctx.getCurrentKey + "】" + ctx.timerService().currentWatermark() + "连续5s水位上涨")
        }

        //每来一条数据，方法会触发执行一次
        override def processElement(value: Sensor, //输入数据
                                    ctx: KeyedProcessFunction[String, Sensor, String]#Context, //上下文环境
                                    out: Collector[String]): Unit = { //输出数据
          if (value.vc > currentHeight) {
            //如果传感器的水位值大于上一次的水位，那么准备触发定时器
            if (alarmTimer == 0L) {
              //准备定时器
              alarmTimer = value.ts * 1000 + 5000
              ctx.timerService().registerEventTimeTimer(alarmTimer)
            }
          } else {
            //如果传感器的水位值小于或等于上一次的水位，那么重置定时器
            ctx.timerService().deleteEventTimeTimer(alarmTimer)
            alarmTimer = value.ts * 1000 + 5000
            ctx.timerService().registerEventTimeTimer(alarmTimer)
          }
          currentHeight = value.vc.toLong
        }
      }
    )

    //打印
    wsDS.print("water>>>")
    processDS.print("process>>>")

    //执行流
    env.execute()
  }
}
