package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.AppMarketAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class AppMarketAnalysisService extends TService{
  private val dao = new AppMarketAnalysisDao

  override def getDao(): TDao = dao

  override def analyses() = {
    //获取市场渠道推广数据
    val dataDS: DataStream[bean.MarketingUserBehavior] = dao.mockData

    val timeDS: DataStream[bean.MarketingUserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp)

    //将数据进行统计：（channel + 用户行为，1）
    val kvDS: DataStream[(String, Int)] = timeDS.map(
      data => {
        (data.channel + "_" + data.behavior, 1)
      }
    )


    val result: DataStream[String] = kvDS
      .keyBy(_._1)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .process(
        new ProcessWindowFunction[(String, Int), String, String, TimeWindow] {
          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
            out.collect(context.window.getStart + "-" + context.window.getEnd + ",APP安装量=" + elements)
          }
        }
      )


    result
  }
}
