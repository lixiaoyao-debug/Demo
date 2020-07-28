package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.HotResourcesAnalysesService
import org.apache.flink.streaming.api.scala.DataStream

class HotResourcesAnalysesController extends TController{
  private val service = new HotResourcesAnalysesService
  override def execute(): Unit = {
    val result: DataStream[String] = service.analyses()
    result.print()
  }
}
