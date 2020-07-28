package com.ll.flink.service

import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.ll.flink.bean.{ApachLogEvent, UrlCount}
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.HotResourceAnalysisDao
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

class HotResourceAnalysisService extends TService {
  private val dao = new HotResourceAnalysisDao

  override def getDao(): TDao = dao

  override def analysis() = {
    //获取服务器日志数据
    val dataDS: DataStream[String] = dao.readFile()

    //将数据进行封装
    val apachLogDS: DataStream[ApachLogEvent] = dataDS.map(
      log => {
        val datas: Array[String] = log.split(" ")
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        ApachLogEvent(
          datas(0),
          datas(1),
          sdf.parse(datas(3)).getTime,
          datas(5),
          datas(6)
        )
      }
    )

    //抽取事件时间和水位线标记,以为日志中时间是无序的，故用函数assignTimestampsAndWatermarks
    val waterDS: DataStream[ApachLogEvent] = apachLogDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApachLogEvent](Time.minutes(1)) {
        override def extractTimestamp(element: ApachLogEvent): Long = {
          element.eventTime
        }
      }
    )

    //根据url进行分流
    val urlKS: KeyedStream[ApachLogEvent, String] = waterDS.keyBy(_.url)

    //设置窗口范围,timeWindow用于KeyedStream
    val urlWS: WindowedStream[ApachLogEvent, String, TimeWindow] = urlKS.timeWindow(Time.minutes(10), Time.seconds(5))

    //将数据进行聚合操作,aggregate用于WindowedStream
    val aggreDS: DataStream[UrlCount] = urlWS.aggregate(
      new AggregateFunction[ApachLogEvent, Long, Long] {
        override def createAccumulator(): Long = 0L
        override def add(in: ApachLogEvent, acc: Long): Long = acc + 1L
        override def getResult(acc: Long): Long = acc
        override def merge(acc: Long, acc1: Long): Long = acc + acc1
      },
      new WindowFunction[Long, UrlCount, String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlCount]): Unit = {
          out.collect(UrlCount(key, window.getEnd, input.iterator.next()))
        }
      }
    )

    //进行聚合操作之后数据打散到了一个窗口，再根据windowEndTime进行分流
    val aggreKS: KeyedStream[UrlCount, Long] = aggreDS.keyBy(_.windowEnd)

    //将分流后的数据再进行处理
    val processDS: DataStream[String] = aggreKS.process(
      new KeyedProcessFunction[Long, UrlCount, String] {
        //有状态的数据集合
        private var resourceList: ListState[UrlCount] = _
        //有状态的定时器
        private var alarmTimer: ValueState[Long] = _

        //初始化状态
        override def open(parameters: Configuration): Unit = {
          resourceList=getRuntimeContext.getListState(new ListStateDescriptor[UrlCount]("resourceList", classOf[UrlCount]))
          alarmTimer=getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTimer", classOf[Long]))
        }

        //定时器触发
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlCount, String]#OnTimerContext, out: Collector[String]): Unit = {
          //到达定时器触发时i，进行数据排序输出
          val datas: lang.Iterable[UrlCount] = resourceList.get()
          val list = new ListBuffer[UrlCount]
          import scala.collection.JavaConversions._
          for (data <- datas) {
            list.add(data)
          }

          //清除状态数据
          resourceList.clear()
          alarmTimer.clear()

          val sortedBuffer: ListBuffer[UrlCount] = list.sortWith(
            (left, right) => {
              left.count > right.count
            }
          ).take(3)

          val builder = new StringBuilder
          builder.append("当前时间：" + new Timestamp(timestamp) + "\n")
          for (data <- sortedBuffer) {
            builder.append("URL:" + data.url + ",点击数量：" + data.count + "\n")
          }
          builder.append("=================================")
          out.collect(builder.toString())
          Thread.sleep(10000)
        }

        override def processElement(value: UrlCount, ctx: KeyedProcessFunction[Long, UrlCount, String]#Context, out: Collector[String]): Unit = {
          //保存数据的状态
          resourceList.add(value)
          //设置定时器
          if (alarmTimer.value() == 0) {
            ctx.timerService().registerEventTimeTimer(value.windowEnd)
            alarmTimer.update(value.windowEnd)
          }
        }
      }
    )

    processDS

  }
}
