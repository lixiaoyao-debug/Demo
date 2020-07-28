package com.ll.flink.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlinkStreamEnv {
  private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

  //环境初始化
  def init()={
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    envLocal.set(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env
  }

  //获取环境
  def get() ={
    var env: StreamExecutionEnvironment = envLocal.get()
    if(env==null){
      env=init()
    }
    env
  }

  //清空环境
  def clear() ={
    envLocal.remove()
  }

  //执行环境
  def execute()={
    get().execute("application")
  }
}
