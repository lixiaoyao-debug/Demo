package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.PageViewAnalysesController

/**
 * 页面访问量统计分析
 */
object PageViewAnalysesApplication extends App with TApplication{
start{
  val controller = new PageViewAnalysesController
  controller.execute()
}
}
