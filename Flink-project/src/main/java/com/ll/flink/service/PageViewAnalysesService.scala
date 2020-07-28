package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.PageViewAnalysesDao
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class PageViewAnalysesService extends TService{
  private val dao = new PageViewAnalysesDao

  override def getDao(): TDao = dao

  override def analyses() = {
    //获取用户数据
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()
    val timeDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //将数据过滤，保留pv数据
    val pvDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //将过滤后的数据进行结构转换：（pv,1）
    val pvToOneDS: DataStream[(String, Int)] = pvDS.map(pv => ("pv", 1))

    //将相同key进行分组
    val pvToOneKS: KeyedStream[(String, Int), String] = pvToOneDS.keyBy(_._1)

    //设定窗口范围
    val pvToOneWS: WindowedStream[(String, Int), String, TimeWindow] = pvToOneKS.timeWindow(Time.hours(1))

    //数据聚合
    val pvToSumDS: DataStream[(String, Int)] = pvToOneWS.sum(1)

    pvToSumDS
  }
}
