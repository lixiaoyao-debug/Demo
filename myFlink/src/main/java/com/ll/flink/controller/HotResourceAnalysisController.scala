package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.HotResourceAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class HotResourceAnalysisController extends TController{
  private val service = new HotResourceAnalysisService

  override def execute(): Unit = {
    val result: DataStream[String] = service.analysis()
    result.print()
  }
}
