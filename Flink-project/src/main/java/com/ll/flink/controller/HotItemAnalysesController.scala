package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.HotItemAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

/**
 * 热门商品分析的控制器
 */
class HotItemAnalysesController extends TController{
  private val service = new HotItemAnalysesService

  /**
   * 执行
   */
 override def execute()={
   val result: DataStream[String] = service.analyses()
   result.print()
  }
}
