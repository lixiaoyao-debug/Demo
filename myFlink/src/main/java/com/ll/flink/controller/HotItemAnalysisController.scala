package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.HotItemAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class HotItemAnalysisController extends TController{
  private val service = new HotItemAnalysisService
  override def execute() = {
    val result: DataStream[String] = service.analysis()
    result.print()
  }
}
