package com.ll.flink

package object bean {
  /**
   * 用户行为数据
   */
  case class UserBehavior(
                         userId:Long,
                         itemId:Long,
                         categroyId:Long,
                         behavior:String,
                         timestamp:Long
                         )

  /**
   * 热门商品点击
   * @param itemId
   * @param clickCount
   */
  case class HotItemClick(
                          itemId:Long,
                          clickCount:Long,
                          windowEndTime:Long
                          )

  /**
   * 服务器日志对象
   * @param ip
   * @param userId
   * @param eventTime
   * @param method
   * @param url
   */
  case class ApacheLog(
                      ip:String,
                      userId:String,
                      eventTime:Long,
                      method:String,
                      url:String
                      )

  /**
   * 热门资源点击
   * @param url
   * @param clickCount
   */
  case class HotResourceClick(
                           url:String,
                           clickCount:Long,
                           windowEndTime:Long
                         )

  /**
   * 市场推广数据
   */
  case class MarketingUserBehavior(
                                    userId: Long,
                                    behavior: String,
                                    channel: String,
                                    timestamp: Long)


  /**
   * 广告点击的日志数据
   * @param userId
   * @param adId
   * @param province
   * @param city
   * @param timestamp
   */
  case class AdClickLog(userId: Long,
                        adId: Long,
                        province: String,
                        city: String,
                        timestamp: Long)

  case class CountByProvince(windowEnd: String,
                             province: String,
                             adId:Long,
                             count: Long)

  /**
   * 登陆数据
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


  case class OrderEvent( orderId: Long, eventType: String, txId: String, eventTime: Long )

  case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )


}
