package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.bean.{AdClickLog, CountByProvince}
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.AdClickAnalysisDao
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AdClickAnalysisService extends TService {
  private val dao = new AdClickAnalysisDao

  override def getDao(): TDao = dao

  /**
   * 广告点击统计
   * @return
   */
  override def analyses() = {
    //获取数据源
    val dataDS: DataStream[String] = dao.readTextFile("input/AdClickLog.csv")

    //将数据封装
    val logDS: DataStream[AdClickLog] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        bean.AdClickLog(
          datas(0).toLong,
          datas(1).toLong,
          datas(2),
          datas(3),
          datas(4).toLong
        )
      }
    )
    //抽取时间戳和水位线标记
    val timeDS: DataStream[AdClickLog] = logDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //((provice,adv)-Count)
    //将数据进行统计
    val ds: DataStream[CountByProvince] = timeDS.map(
      log => {
        (log.province + "_" + log.adId, 1L)
      }
    ).keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(
        new AggregateFunction[(String, Long), Long, Long] {
          override def createAccumulator(): Long = 0L

          override def add(in: (String, Long), acc: Long): Long = {
            acc + 1L
          }

          override def getResult(acc: Long): Long = acc

          override def merge(acc: Long, acc1: Long): Long = {
            acc + acc1
          }
        },
        new WindowFunction[Long, CountByProvince, String, TimeWindow] {
          override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
            val ks: Array[String] = key.split("_")
            val count = input.iterator.next()
            out.collect(CountByProvince(
              window.getEnd.toString,
              ks(0),
              ks(1).toLong,
              count
            ))
          }
        }
      )
    ds



  }
}
