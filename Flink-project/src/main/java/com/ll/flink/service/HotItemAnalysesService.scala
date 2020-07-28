package com.ll.flink.service

import com.ll.flink.bean
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.HotItemAnalysesDao
import com.ll.flink.function.{HotItemAnalysesAggregateFunction, HotItemProcessFunction, HotItemWindowFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class HotItemAnalysesService extends TService {
  private val dao = new HotItemAnalysesDao

  override def getDao(): TDao = dao


  /**
   * 分析
   */
  override def analyses() = {

    //1.获取用户数据行为数据
    val ds: DataStream[bean.UserBehavior] = getUserBehaviorDatas()

    //2.设置时间戳和水位线
    val timeDS: DataStream[bean.UserBehavior] = ds.assignAscendingTimestamps(_.timestamp * 1000L)

    //3.将数据进行清洗，保留点击数据
    val filterDS: DataStream[bean.UserBehavior] = timeDS.filter(_.behavior == "pv")

    //4.将相同商品聚合一起
    val dataKS: KeyedStream[bean.UserBehavior, Long] = filterDS.keyBy(_.itemId)

    //5.设定时间窗口
    val dataWS: WindowedStream[bean.UserBehavior, Long, TimeWindow] = dataKS.timeWindow(Time.hours(1), Time.minutes(5))

    //TODO 6.聚合数据
    //单一数据处理
    //    dataWS.aggregate()
    //全量数据处理
    //    dataWS.process()

    //6.1 聚合数据
    //6.2 将聚合的结果进行转换，方便排序
    //aggregate需要两个参数
    //第一个参数表示聚合函数
    //第二个函数表示当前窗口的处理函数
    //第一个函数的处理结果会作为第二个函数输入值进行传递

    //当窗口数据进行聚合后，会将所有窗口数据全部打乱，获取总的数据
    val hicDS: DataStream[bean.HotItemClick] = dataWS.aggregate(
      new HotItemAnalysesAggregateFunction,
      new HotItemWindowFunction
    )

    //7.将数据根据窗口进行分组
    val hicKS: KeyedStream[bean.HotItemClick, Long] = hicDS.keyBy(_.windowEndTime)

    //8.对聚合后的数据进行排序
    val result: DataStream[String] = hicKS.process(new HotItemProcessFunction)

    result

  }
}
