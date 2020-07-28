package com.ll.flink.dao

import com.ll.flink.bean.MarketingUserBehavior
import com.ll.flink.common.TDao
import com.ll.flink.util.FlinkStreamEnv
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

class AppMarketAnalysisDao extends TDao{

  //生成模拟数据
  def mockData={
    //创建自定义数据源
    FlinkStreamEnv.get().addSource(
      new SourceFunction[MarketingUserBehavior] {
        var runflag=true
        override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
          while(runflag){
            ctx.collect(MarketingUserBehavior(1,"INSTALL","HUAWEI",System.currentTimeMillis()))
            Thread.sleep(500)
          }
        }

        override def cancel(): Unit = {
          runflag=false
        }
      }
    )
  }
}
