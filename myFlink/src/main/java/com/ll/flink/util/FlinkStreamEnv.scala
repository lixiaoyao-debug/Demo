package com.ll.flink.util

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Flink流环境
 */
object FlinkStreamEnv {

  //线程共享内存
  private val envLocal = new ThreadLocal[StreamExecutionEnvironment]

  //初始化环境
  def init() = {
    //创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //将环境放入共享内存
    envLocal.set(env)
    //设置时间定义为事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env
  }

  //获取环境
  def get() = {
    var env: StreamExecutionEnvironment = envLocal.get()
    if (env == null) {
      env = init()
    }
    env
  }

  //清空共享区域
  def clear()={
    envLocal.remove()
  }

  //执行环境
  def execute() = {
    get().execute("myDemo")
  }
}
