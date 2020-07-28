package com.ll.flink.service

import com.ll.flink.bean.{AdClickLog, CountByProvince}
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.AdAnalysisDao
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdAnalysisService extends TService {
  private val dao = new AdAnalysisDao

  override def getDao(): TDao = dao

  override def analysis() = {
    //获取数据源信息
    val dataDS: DataStream[String] = dao.readFile()

    //将数据转换为AdClickLog
    val adLogDS: DataStream[AdClickLog] = dataDS.map(
      log => {
        val datas: Array[String] = log.split(",")
        AdClickLog(
          datas(0).toLong,
          datas(1).toLong,
          datas(2),
          datas(3),
          datas(4).toLong
        )
      }
    )

    //设置时间戳和水位线标记,由于文件中的事件时间是单调递增的，所以用函数assignAscendingTimestamps
    val timeDS: DataStream[AdClickLog] = adLogDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //将adLogDS进行分流开窗再聚合
    val result: DataStream[CountByProvince] = timeDS
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(
        new AggregateFunction[AdClickLog, Long, Long] {
          override def createAccumulator(): Long = 0L

          override def add(in: AdClickLog, acc: Long): Long = acc + 1L

          override def getResult(acc: Long): Long = acc

          override def merge(acc: Long, acc1: Long): Long = acc + acc1
        },
        new WindowFunction[Long, CountByProvince, String, TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
            out.collect(CountByProvince(window.getEnd.toString, key, input.iterator.next()))
          }
        }
      )

    result

  }
}
