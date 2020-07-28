package com.ll.flink.controller

import com.ll.flink.common.TController
import com.ll.flink.service.LoginFailAnalysisService
import org.apache.flink.streaming.api.scala.DataStream

class LoginFailAnalysisController extends TController{
  private val service = new LoginFailAnalysisService
  override def execute(): Unit = {
    val result: DataStream[String] = service.analyses()
    result.print()
  }
}
