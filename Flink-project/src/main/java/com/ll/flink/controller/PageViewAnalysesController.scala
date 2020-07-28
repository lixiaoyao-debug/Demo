package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.PageViewAnalysesService

class PageViewAnalysesController extends TController{
  private val service = new PageViewAnalysesService
  override def execute()= {
    val result= service.analyses()
    result.print()
  }
}
