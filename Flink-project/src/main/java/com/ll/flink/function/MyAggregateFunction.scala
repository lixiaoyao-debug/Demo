package com.ll.flink.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * 自定义的累加函数
 */
class MyAggregateFunction[T] extends AggregateFunction[T, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: T, acc: Long): Long = {
    acc + 1L
  }

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
