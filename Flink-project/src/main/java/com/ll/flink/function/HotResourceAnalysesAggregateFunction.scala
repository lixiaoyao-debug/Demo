package com.ll.flink.function

import com.ll.flink.bean.{ApacheLog, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction

class HotResourceAnalysesAggregateFunction extends AggregateFunction[ApacheLog, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLog, acc: Long): Long = {
    acc + 1L
  }

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}