package com.ll.flink

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Flink_TimeWatermark {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度
    //如果设置并行度不为1，那么在计算窗口时，是按照不同并行度单独计算的
    env.setParallelism(1)

    //设置时间语义为事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //水位线(水印)
    //其实是对迟到数据的处理机制，当watermark达到了数据窗口的结束时间时，触发窗口计算
    //需要将计算时间点推迟，推迟到watermark标记的时间
//    val dataDS: DataStream[String] = env.socketTextStream("hadoop166", 9999)

    //如果数据源时file,那么在读取数据后进行计算时
    //即使数据窗口没有提交，那么在文件读取结束后也会自动计算
    //因为flink会在文件读完后，将watermark设置为Long的最大值
    //需要将所有未计算的窗口全部进行计算
    val dataDS: DataStream[String] = env.readTextFile("input/sensor-data.txt")
    val sensorDS: DataStream[Sensor] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        Sensor(datas(0), datas(1).toLong, datas(2).toDouble)
      }
    )

    //抽取时间戳和设定水平线（标记）
    //1.从数据中抽取数据作为事件时间
    //2.设定水位线标记，这个标记一般比当前数据事件事件要推迟
    val markDS: DataStream[Sensor] = sensorDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[Sensor](Time.seconds(3)) {
        override def extractTimestamp(element: Sensor): Long = {
          //抽取事件时间，以毫秒为单位
          element.ts * 1000L
        }
      }
    )

    val outputTag=new OutputTag[Sensor]("lateDate")

    val applyDS: DataStream[String] = markDS.keyBy(_.id)
      .timeWindow(Time.seconds(5))
      //如果watermark已经触发了窗口的计算，那么这个窗口就不会再收到数据，再重新结算
      //如果窗口计算完毕后，还想对迟到的数据进行处理，可以让窗口的计算再晚一些
      //让窗口可以接收迟到数据（不是watermark概念）：allowedLateness
      .allowedLateness(Time.seconds(2))
      //如果指定的窗口已经计算完毕，不再接收新的数据，原则上讲不再接收的数据就会丢弃
      //如果必要统计，可是窗口又不再接收，那么可以将数据放置在一个特殊的流中
      //这个流称之为侧输出流：SideOutput
      .sideOutputLateData(outputTag)
      .apply(
        //key:分流的key
        //window:当前使用窗口的类型
        //datas:窗口中的数据
        //out:输出，指定输出的类型
        (key: String, window: TimeWindow, datas: Iterable[Sensor], out: Collector[String]) => {
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val start: Long = window.getStart
          val end: Long = window.getEnd
          out.collect(s"[$start-$end),数据[$datas]")
        }
      )

    //打印
    markDS.print("mark>>>")
    //计算正确的数据窗口
    applyDS.print("window>>>")
    //将晚到的数据进行计算
    val lateDataDS: DataStream[Sensor] = applyDS.getSideOutput(outputTag)
    lateDataDS.print("side>>>")

    //执行流
    env.execute()

  }
}
