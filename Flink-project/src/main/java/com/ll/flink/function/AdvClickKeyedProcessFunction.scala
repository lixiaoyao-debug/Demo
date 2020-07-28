package com.ll.flink.function

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class AdvClickKeyedProcessFunction extends KeyedProcessFunction[String,(String,Long),(String,Long)]{
  override def processElement(value: (String, Long), ctx: KeyedProcessFunction[String, (String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = ???
}
