package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.HotItemAnalysisDao
import com.ll.flink.function.{HotItemAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemAnalysisService extends TService {

  private val dao = new HotItemAnalysisDao

  override def getDao(): TDao = dao

  /**
   * 数据分析
   *
   * @return
   */
  override def analysis() = {
    //获取用户行为数据
    val dataDS: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    //设置时间戳和水位线标记(确定了数据流的时间戳是单调递增的，
    // 所以使用assignAscendingTimestamps，这个方法直接使用数据的时间戳生成watermark)
    val timeDS: DataStream[bean.UserBehavior] = dataDS.assignAscendingTimestamps(_.timestamp * 1000L)

    //过滤数据，保留点击数据
    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //将相同品类ID聚合在一起
    val dataKS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //设置时间窗口
    val dataWS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = dataKS.timeWindow(Time.hours(1), Time.minutes(5))

    //当窗口数据进行聚合后，会将所有窗口数据全部打乱，获取总的数据
    val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(
      new HotItemAggregateFunction,
      new HotItemWindowFunction
    )

    //将数据根据窗口重新分组
    val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)

    //对聚合后的函数进行排序
    val resultDS: DataStream[String] = hicKS.process(new HotItemProcessFunction)

    resultDS
  }
}
