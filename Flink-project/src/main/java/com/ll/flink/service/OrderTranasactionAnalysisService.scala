package com.ll.flink.service

import com.ll.flink.bean.{OrderEvent, ReceiptEvent}
import com.ll.flink.common.{TDao, TService}
import com.ll.flink.dao.OrderTranasactionAnalysisDao
import org.apache.flink.streaming.api.scala._

class OrderTranasactionAnalysisService extends TService{
  private val dao = new OrderTranasactionAnalysisDao
  override def getDao(): TDao = dao

  override def analyses() = {
    //获取两个不同的数据流
    val dataDS1: DataStream[String] = getDao().readTextFile("input/OrderLog.csv")
    val dataDS2: DataStream[String] = getDao().readTextFile("input/ReceiptLog.csv")

    val oeDS: DataStream[OrderEvent] = dataDS1.map(
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
    oeDS.assignAscendingTimestamps(_.eventTime*1000L)

    val reDS: DataStream[ReceiptEvent] = dataDS2.map(
      data => {
        val datas: Array[String] = data.split(",")
        ReceiptEvent(
          datas(0),
          datas(1),
          datas(2).toLong
        )
      }
    )
    reDS

  }


}
