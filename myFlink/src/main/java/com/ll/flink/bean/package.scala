package com.ll.flink

package object bean {

  /**
   * 用户行为数据
   */
  case class UserBehavior(
                           userId: Long,
                           itemId: Long,
                           categoryId: Long,
                           behavior: String,
                           timestamp: Long
                         )

  /**
   * 热门商品点击
   */
  case class HotItemClick(
                           itemId: Long,
                           clickCount: Long,
                           windowEndTime: Long
                         )

  /**
   * 热门数据源事件日志
   */
  case class ApachLogEvent(
                            ip: String,
                            userId: String,
                            eventTime: Long,
                            method: String,
                            url: String
                          )

  /**
   * url访问次数
   */
  case class UrlCount(
                       url: String,
                       windowEnd: Long,
                       count: Long
                     )

  /**
   * 广告点击日志
   *
   * @param userId
   * @param adId
   * @param province
   * @param city
   * @param timestamp
   */
  case class AdClickLog(
                         userId: Long,
                         adId: Long,
                         province: String,
                         city: String,
                         timestamp: Long)

  /**
   * 省份点击次数
   *
   * @param windowEnd
   * @param province
   * @param count
   */
  case class CountByProvince(
                              windowEnd: String,
                              province: String,
                              count: Long)

  /**
   * 登陆数据
   *
   * @param userId
   * @param ip
   * @param eventType
   * @param eventTime
   */
  case class LoginEvent(
                         userId: Long,
                         ip: String,
                         eventType: String,
                         eventTime: Long)

  /**
   * 订单数据
   *
   * @param orderId
   * @param eventType
   * @param txId
   * @param eventTime
   */
  case class OrderEvent(
                         orderId: Long,
                         eventType: String,
                         txId: String,
                         eventTime: Long)

  /**
   * 订单结果
   * @param orderId
   * @param eventType
   */
  case class OrderResult(
                          orderId: Long,
                          eventType: String)


}
