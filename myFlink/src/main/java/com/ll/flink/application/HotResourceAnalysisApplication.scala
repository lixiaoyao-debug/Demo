package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.HotResourceAnalysisController

object HotResourceAnalysisApplication extends App with TApplication {
  start {
    val controller = new HotResourceAnalysisController
    controller.execute()
  }
}
