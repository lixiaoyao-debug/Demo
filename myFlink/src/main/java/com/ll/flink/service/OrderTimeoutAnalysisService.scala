package com.ll.flink.service

import com.ll.flink.bean.{OrderEvent, OrderResult}
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.OrderTimeoutAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala
import org.apache.flink.table.api.scala.map
import org.apache.flink.util.Collector

class OrderTimeoutAnalysisService extends TService {
  private val dao = new OrderTimeoutAnalysisDao

  override def getDao(): TDao = dao

  def analysisNormal() = {
    //获取订单交易数据
    val dataDS: DataStream[String] = getDao().readFile()

    //将其转换为OrderEvent
    val orderDS: DataStream[OrderEvent] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        OrderEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    )

    //设置时间戳和水位线,由于文件中时间是单调递增的，所以应用函数assignTimestampsAndWatermarks
    val orderTimeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

    val orderKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

    //定义规则
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("followed")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    //应用规则
    val orderPS: PatternStream[OrderEvent] = CEP.pattern(orderKS, pattern)

    //获取结果
    orderPS.select(
      map => {
        val order: OrderEvent = map("begin").iterator.next()
        val pay: OrderEvent = map("followed").iterator.next()
        var s = "订单ID：" + order.orderId
        s += "共耗时" + (pay.eventTime - order.eventTime) + "秒"
        s
      }
    )
  }

  override def analysis() = {
    analysisNormal
  }

  def analysesTimeoutNoCEP() = {
    //获取订单交易数据
    val dataDS: DataStream[String] = getDao().readFile()

    //将其转换为OrderEvent
    val orderDS: DataStream[OrderEvent] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        OrderEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    )

    //设置时间戳和水位线,由于文件中时间是单调递增的，所以应用函数assignTimestampsAndWatermarks
    val orderTimeDS: DataStream[OrderEvent] = orderDS.assignAscendingTimestamps(_.eventTime * 1000L)

    val orderKS: KeyedStream[OrderEvent, Long] = orderTimeDS.keyBy(_.orderId)

    //TODO 一条数据来了就做处理
    //因为使用keyBy,所以相同的订单数据会到一个流中
    orderKS.process(
      new KeyedProcessFunction[Long, OrderEvent, String] {
        private var orderMergePay: ValueState[OrderResult] = _
        private var alarmTimer: ValueState[Long] = _

        //初始化状态
        override def open(parameters: Configuration): Unit = {
          orderMergePay=getRuntimeContext.getState(new ValueStateDescriptor[OrderResult]("OrderResult",classOf[OrderResult]))
          alarmTimer=getRuntimeContext.getState(new ValueStateDescriptor[Long]("alarmTimer",classOf[Long]))
        }

        //设置定时器
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
          //定时器如果触发，说明数据无法在指定时间范围内到达，做特殊处理
          val mergeData: OrderResult = orderMergePay.value()
          val outputTag = new OutputTag[String]("timeout")
//          if(mergeData.or)
        }
        override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, String]#Context, out: Collector[String]): Unit = ???
      }
    )
  }
}
