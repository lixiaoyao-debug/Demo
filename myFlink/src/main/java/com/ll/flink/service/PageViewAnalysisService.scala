package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.PageViewAnalysisDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

class PageViewAnalysisService extends TService {
  private val dao = new PageViewAnalysisDao

  override def getDao(): TDao = dao


  //分析
  override def analysis() = {
    //获取数据源
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    //设置时间戳和水位线,由于一直日志事件事件为单调递增，故用函数assignAscendingTimestamps
    val timeDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //对整个日志进行过滤，仅过滤出pv数据
    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //对pv数据进行转换聚合计算
    val result: DataStream[(String, Int)] = filterDS.map(
      data => {
        (data.behavior, 1)
      }
    ).keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    result

  }
}
