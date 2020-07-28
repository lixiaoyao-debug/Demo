package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.OrderTimeoutAnalysisController

object OrderTimeoutAnalysisApplication extends App with TApplication {
  start {
    val controller = new OrderTimeoutAnalysisController
    controller.execute()
  }
}
