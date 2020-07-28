package com.ll.flink.controller

import com.ll.flink.bean
import com.ll.flink.common.TController
import com.ll.flink.service.AppMarketAnalysisService
import org.apache.flink.streaming.api.scala.DataStream


class AppMarketAnalysisController extends TController{
  private val service = new AppMarketAnalysisService
  override def execute(): Unit = {
    val result: DataStream[String] = service.analyses()
    result.print()
  }
}
