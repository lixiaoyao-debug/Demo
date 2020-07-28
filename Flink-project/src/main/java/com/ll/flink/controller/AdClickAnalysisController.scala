package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.AdClickAnalysisService

class AdClickAnalysisController extends TController{
  private val seervice = new AdClickAnalysisService
  override def execute(): Unit = {
    val result= seervice.analyses()
    result.print
  }
}
