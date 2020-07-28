package com.ll.flink.common

import com.ll.flink.util.FlinkStreamEnv

trait TApplication {
  //控制抽象，把一段代码作为逻辑传入
  def start(op: => Unit):Unit = {
    try {
      //初始化环境
      FlinkStreamEnv.init()

      //逻辑代码
      op

      //执行环境
      FlinkStreamEnv.execute()
    } catch {
      case e => e.printStackTrace()
    } finally {
      //清空共享环境
      FlinkStreamEnv.clear()
    }
  }
}
