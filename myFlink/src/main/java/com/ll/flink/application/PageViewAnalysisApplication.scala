package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.PageViewAnalysisController

object PageViewAnalysisApplication extends App with TApplication {
  start {
    val controller = new PageViewAnalysisController
    controller.execute()
  }
}
