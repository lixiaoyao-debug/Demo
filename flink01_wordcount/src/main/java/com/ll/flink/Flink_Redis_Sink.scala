package com.ll.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object Flink_Redis_Sink {
  def main(args: Array[String]): Unit = {
    //创建上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    env.setParallelism(1)

    //Source
    val sourceDS: DataStream[String] = env.fromCollection(List("Never", "Give", "Up"))

    //向Redis输出内容
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop166").setPort(6379).build()
    sourceDS.addSink(
      new RedisSink[String](
        conf,
        new RedisMapper[String] {
          override def getCommandDescription: RedisCommandDescription = {
            new RedisCommandDescription(
              RedisCommand.HSET,
              "sensor"
            )
          }

          override def getKeyFromData(t: String): String = {
            "hashKey:"+t
          }


          override def getValueFromData(t: String): String = {
            "hashValue:"+t
          }
        }
      )
    )

    //执行流
    env.execute()
  }

}
