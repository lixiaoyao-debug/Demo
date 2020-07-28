package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.FailLoginAnalysisController

object FailLoginAnalysisApplication extends App with TApplication {
  start {
    val controller = new FailLoginAnalysisController
    controller.execute()
  }
}
