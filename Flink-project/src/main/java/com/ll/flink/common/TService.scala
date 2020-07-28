package com.ll.flink.common

import com.ll.flink.bean.UserBehavior
import com.ll.flink.service.path
import org.apache.flink.streaming.api.scala._

trait TService {

  def getDao():TDao

  //分析
  def analyses():Any

  def getUserBehaviorDatas()={
    val dataDS: DataStream[String] = getDao.readTextFile("input/UserBehavior.csv")
//val dataDS: DataStream[String] = getDao().readKafka()

    //TODO 1.将原始数据进行封装对象，方便使用
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
