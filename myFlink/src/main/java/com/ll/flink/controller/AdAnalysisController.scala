package com.ll.flink.controller

import com.ll.flink.bean
import com.ll.flink.common.TController
import com.ll.flink.service.AdAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class AdAnalysisController extends TController{
  private val service = new AdAnalysisService

  override def execute(): Unit = {
    val result: DataStream[bean.CountByProvince] = service.analysis()
    result.print()
  }
}
