package com.ll.flink.common

import com.ll.flink.util.FlinkStreamEnv

trait TApplication {
  def start(op: => Unit): Unit = {
    try {
      //初始化环境
      FlinkStreamEnv.init()
      op
      //执行环境
      FlinkStreamEnv.execute()
    } catch {
      case e => e.printStackTrace()
    }finally {
      //最后清空环境
      FlinkStreamEnv.clear()
    }
  }
}
