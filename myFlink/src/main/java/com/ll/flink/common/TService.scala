package com.ll.flink.common

import com.ll.flink.bean.UserBehavior
import org.apache.flink.streaming.api.scala._

/**
 * 通用服务特质
 */
trait TService {
  def getDao(): TDao

  //分析
  def analysis(): Any

  /**
   * 获取用户行为的封装数据
   */
  protected def getUserBehaviorDatas() = {
        val dataDS: DataStream[String] = getDao().readFile()
//    val dataDS: DataStream[String] = getDao().readKafka()
    //将原始数据进行封装对象，方便后续使用
    val userBehaviorDS: DataStream[UserBehavior] = dataDS.map(
      data => {
        val datas: Array[String] = data.split(",")
        UserBehavior(
          datas(0).toLong,
          datas(1).toLong,
          datas(2).toLong,
          datas(3),
          datas(4).toLong
        )
      }
    )
    userBehaviorDS
  }
}
