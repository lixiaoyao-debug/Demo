package com.ll.flink

import java.util.Properties

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class MySensorSource extends SourceFunction[Sensor] {
  var flag = true

  //采集
  override def run(sourceContext: SourceFunction.SourceContext[Sensor]): Unit = {
    while (flag) {
      sourceContext.collect(
        Sensor(
          "sensor_" + new Random().nextInt(10),
          System.currentTimeMillis(),
          new Random().nextInt(5)
        )
      )
      Thread.sleep(1000)
    }
  }

  //取消
  override def cancel(): Unit = {
    flag = false
  }
}
