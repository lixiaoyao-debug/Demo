package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.PageViewAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class PageViewAnalysisController extends TController{
  private val service = new PageViewAnalysisService
  override def execute(): Unit = {
    val result: DataStream[(String, Int)] = service.analysis()
    result.print()
  }
}
