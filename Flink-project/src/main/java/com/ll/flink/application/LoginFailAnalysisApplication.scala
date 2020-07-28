package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.LoginFailAnalysisController

object LoginFailAnalysisApplication extends App with TApplication {
  start {
    val controller = new LoginFailAnalysisController
    controller.execute()
  }
}
