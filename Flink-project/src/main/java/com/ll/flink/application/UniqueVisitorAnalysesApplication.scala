package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.UniqueVisitorAnalysesController

object UniqueVisitorAnalysesApplication extends App with TApplication {
  start {
    val controller = new UniqueVisitorAnalysesController
    controller.execute()
  }
}
