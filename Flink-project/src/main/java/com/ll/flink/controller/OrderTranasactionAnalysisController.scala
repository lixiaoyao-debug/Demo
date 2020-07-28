package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.OrderTranasactionAnalysisService

class OrderTranasactionAnalysisController extends TController{
  private val service = new OrderTranasactionAnalysisService
  override def execute(): Unit = {
    service.analyses()
  }
}
