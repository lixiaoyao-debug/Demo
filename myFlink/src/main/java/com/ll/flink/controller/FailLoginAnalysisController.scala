package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.FailLoginAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class FailLoginAnalysisController extends TController{
  private val service = new FailLoginAnalysisService
  override def execute(): Unit = {
    val result: DataStream[String] = service.analysis()
    result.print()
  }
}
