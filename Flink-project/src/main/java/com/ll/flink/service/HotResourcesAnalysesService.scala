package com.ll.flink.service

import java.text.SimpleDateFormat

import com.ll.flink.bean
import com.ll.flink.bean.ApacheLog
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.HotResourcesAnalysesDao
import com.ll.flink.function.{HotResourceKeyedProcessFunction, HotResourceWindowFunction, MyAggregateFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotResourcesAnalysesService extends TService{
  private val dao = new HotResourcesAnalysesDao

  override def getDao(): TDao = dao

  override def analyses() = {
    val dataDS: DataStream[String] = dao.readTextFile("input/apache.txt")
    val logDS: DataStream[ApacheLog] = dataDS.map(
      log => {
        val datas: Array[String] = log.split(" ")

        val sdf = new SimpleDateFormat("dd/MM/yy:HH:mm:ss")

        ApacheLog(
          datas(0),
          datas(1),
          sdf.parse(datas(3)).getTime,
          datas(5),
          datas(6)
        )
      }
    )

    //抽取事件时间和水位线标记
    val waterDS: DataStream[ApacheLog] = logDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[ApacheLog](Time.minutes(1)) {
        override def extractTimestamp(element: ApacheLog): Long = {
          element.eventTime
        }
      }
    )

    //根据资源访问路径进行分组
    val logKS: KeyedStream[ApacheLog, String] = waterDS.keyBy(_.url)

    //增加窗口数据范围
    val logWS: WindowedStream[ApacheLog, String, TimeWindow] = logKS.timeWindow(Time.minutes(10), Time.seconds(5))

    //将数据进行聚合
    val aggDS: DataStream[bean.HotResourceClick] = logWS.aggregate(
      new MyAggregateFunction[ApacheLog],
      new HotResourceWindowFunction
    )

    //根据窗口时间将数据重新进行分组
    val hrcKS: KeyedStream[bean.HotResourceClick, Long] = aggDS.keyBy(_.windowEndTime)

    //将分组后的数据进行处理
    hrcKS.process(new HotResourceKeyedProcessFunction)

  }
}
