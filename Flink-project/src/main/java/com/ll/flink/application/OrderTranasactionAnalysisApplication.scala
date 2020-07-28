package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.OrderTranasactionAnalysisController

object OrderTranasactionAnalysisApplication extends App with TApplication {
  start {
    val controller = new OrderTranasactionAnalysisController
    controller.execute()
  }
}
