package com.ll.flink.function

import com.ll.flink.bean.{HotItemClick, HotResourceClick}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 热门窗口处理函数
 */
class HotResourceWindowFunction extends WindowFunction[Long,HotResourceClick,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[HotResourceClick])= {
    out.collect(HotResourceClick(key,input.iterator.next(),window.getEnd))
  }
}
