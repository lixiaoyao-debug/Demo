package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.HotResourcesAnalysesController

/**
 * 热门资源浏览量排行
 */
object HotResourcesAnalysesApplication extends App with TApplication {
  start {
    val controller = new HotResourcesAnalysesController
    controller.execute()
  }
}
