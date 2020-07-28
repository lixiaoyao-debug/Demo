package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.AppMarketAnalysisController

object AppMarketAnalysisApplication extends App with TApplication {
  start {
    val controller = new AppMarketAnalysisController
    controller.execute()
  }
}
