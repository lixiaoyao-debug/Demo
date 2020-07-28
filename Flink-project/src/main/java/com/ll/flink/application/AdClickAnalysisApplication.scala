package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.AdClickAnalysisController

object AdClickAnalysisApplication extends App with TApplication {
  start {
    val controller = new AdClickAnalysisController
    controller.execute()
  }
}
