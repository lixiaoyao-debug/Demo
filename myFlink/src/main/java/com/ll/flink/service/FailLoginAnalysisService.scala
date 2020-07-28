package com.ll.flink.service

import com.ll.flink.bean.LoginEvent
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.FailLoginAnalysisDao
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

class FailLoginAnalysisService extends TService {
  private val dao = new FailLoginAnalysisDao

  override def getDao(): TDao = dao

  def analsisNormal() = {

    //获取数据源
    val dataDS: DataStream[String] = getDao().readFile()

    //将原数据转换为LoginEvent
    val loginDS: DataStream[LoginEvent] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        LoginEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    )

    //设置时间戳和水平线，由于日志中事件时间是乱序的，故使用函数
    val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      }
    )

    //对timeDS进行过滤分流判断
    timeDS
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(
        new KeyedProcessFunction[Long, LoginEvent, String] {
          //设置有状态
          private var lastLoginEvent: ValueState[LoginEvent] = _

          //初始化状态
          override def open(parameters: Configuration): Unit = {
            lastLoginEvent = getRuntimeContext.getState(new ValueStateDescriptor[LoginEvent]("lastLoginEvent", classOf[LoginEvent]))
          }

          override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
            val lastEvent: LoginEvent = lastLoginEvent.value()
            if (lastEvent != null) {
              if ((value.eventTime - lastEvent.eventTime) <= 2) {
                out.collect(value.userId + "在连续2秒内登陆失败2次")
              }
            }
            lastLoginEvent.update(value)
          }
        }
      )
  }


  override def analysis() = {
    //TODO 恶意登陆逻辑上的问题
    //1.最开始逻辑就有问题，不应该过滤登陆成功的数据
    //2.如果连续两条数据是乱序的，统计有问题
    //3.如果登陆失败次数大于2次，该如何处理

    //常规逻辑
    analsisNormal

    //CEP
    //    cep

  }

  def cep() = {
    //获取数据源
    val dataDS: DataStream[String] = getDao().readFile()

    //将原数据转换为LoginEvent
    val loginDS: DataStream[LoginEvent] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        LoginEvent(
          datas(0).toLong,
          datas(1),
          datas(2),
          datas(3).toLong
        )
      }
    )

    //设置时间戳和水平线，由于日志中事件时间是乱序的，故使用函数
    val timeDS: DataStream[LoginEvent] = loginDS.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(10)) {
        override def extractTimestamp(element: LoginEvent): Long = {
          element.eventTime * 1000L
        }
      }
    )

    val userKS: KeyedStream[LoginEvent, Long] = timeDS.keyBy(_.userId)

    //定义规则
    val parttern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      //定义时间范围
      .within(Time.seconds(2))

    //应用规则
    val userLoginPS: PatternStream[LoginEvent] = CEP.pattern(userKS, parttern)

    //获取结果
    val result: DataStream[String] = userLoginPS.select(
      map => {
        map.toString()
      }
    )
    result
  }
}
