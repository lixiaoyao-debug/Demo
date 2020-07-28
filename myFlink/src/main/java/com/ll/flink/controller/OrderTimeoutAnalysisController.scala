package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.dao.OrderTimeoutAnalysisDao
import com.ll.flink.service.OrderTimeoutAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class OrderTimeoutAnalysisController extends TController {
  private val service = new OrderTimeoutAnalysisService

  override def execute(): Unit = {
    val result: DataStream[String] = service.analysis()
    result.print()
  }

}
