package com.ll.flink.application

import com.ll.flink.common.TApplication
import com.ll.flink.controller.HotItemAnalysesController

/**
 * 热门的商品统计
 */

object HotItemAnalysesApplication extends App with TApplication{
  start{
    //控制器
    val controller = new HotItemAnalysesController
    controller.execute()
  }
}
