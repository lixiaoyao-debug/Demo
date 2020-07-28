package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.HotItemAnalysisController

object HotItemAnalysisApplication extends App with TApplication{
  start{
    val controller = new HotItemAnalysisController
    controller.execute()
  }
}
