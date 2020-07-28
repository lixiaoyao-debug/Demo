package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.AdAnalysisController

object AdAnalysisApplication extends App with TApplication {
  start {
    val controller = new AdAnalysisController
    controller.execute()
  }

}
