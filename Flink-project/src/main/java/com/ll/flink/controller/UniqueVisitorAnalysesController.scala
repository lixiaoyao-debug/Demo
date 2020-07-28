package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.UniqueVisitorAnalysesService

class UniqueVisitorAnalysesController extends TController {
  private val service = new UniqueVisitorAnalysesService

  override def execute(): Unit = {
    val result = service.analyses()
    result.print()
  }
}
