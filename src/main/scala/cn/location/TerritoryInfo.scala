package cn.location

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerritoryInfo {
  def main(args: Array[String]): Unit = {

    if(args.length !=1) {
      println("输入目录错误")
      sys.exit()
    }

    var Array(inputPath) = args

    val sparkSession = SparkSession
      .builder()
      .appName(this.getClass.getName)
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    val df: DataFrame = sparkSession.read.parquet(inputPath)


    //SQL风格
    //    df.createTempView("log")
    //
    //    sparkSession.sql("select provinceName," +
    //      "cityName," +
    //      "sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) as srcReqNum from log group by provinceName,cityName"
    //      ).show()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._
    //DSL未打标签
//    df.groupBy("provinceName","cityName").agg(
//      sum(when($"requestmode"===1&&$"processnode">=1,1).otherwise(0)) as "srcReqNum",
//      sum(when($"requestmode"===1&&$"processnode">=2,1).otherwise(0)) as "validReqNum",
//      sum(when($"requestmode"===1&&$"processnode"===3,1).otherwise(0)) as "adReqNum",
//      sum(when($"iseffective"===1&&$"isbilling"===1&&$"isbid"===1,1).otherwise(0)) as "biddingNum",
//      sum(when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1&&$"adorderid"!=0,1).otherwise(0)) as "winNum",
//      sum(when($"requestmode"===2&&$"iseffective"===1,1).otherwise(0)) as "showReqNum",
//      sum(when($"requestmode"===3&&$"iseffective"===1,1).otherwise(0)) as "clickReqNum",
//      sum(when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1,$"winprice").otherwise(0.0))/1000 as "DSPAdConsume",
//      sum(when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1,$"adpayment").otherwise(0.0))/1000 as "DSPAdCost"
//    ).show()


    //打标签
    val logMark = df.select($"*",
      when($"requestmode"===1&&$"processnode">=1,1).otherwise(0) as "srcReq",
      when($"requestmode"===1&&$"processnode">=2,1).otherwise(0) as "validReq",
      when($"requestmode"===1&&$"processnode"===3,1).otherwise(0) as "adReq",
      when($"iseffective"===1&&$"isbilling"===1&&$"isbid"===1,1).otherwise(0) as "bidding",
      when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1&&$"adorderid"!=0,1).otherwise(0) as "win",
      when($"requestmode"===2&&$"iseffective"===1,1).otherwise(0) as "showReq",
      when($"requestmode"===3&&$"iseffective"===1,1).otherwise(0) as "clickReq",
      when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1,$"winprice").otherwise(0) as "DSPAdCons",
      when($"iseffective"===1&&$"isbilling"===1&&$"iswin"===1,$"adpayment").otherwise(0) as "DSPAdCo"
    )

    //地域分布
//    logMark.groupBy("provinceName","cityName").agg(
//      sum("srcReq") as "srcReqNum",
//      sum("validReq") as "validReqNum",
//      sum("adReq") as "adReqNum",
//      sum("bidding") as "biddingNum",
//      sum("win") as "winNum",
//      sum("win")/sum("bidding")*1.0 as "winRate",
//      sum("showReq") as "showReqNum",
//      sum("clickReq") as "clickReqNum",
//      sum("clickReq")/sum("showReq") as "clickRate",
//      sum("DSPAdCons")/1000 as "DSPAdConsume",
//      sum("DSPAdCo")/1000 as "DSPAdCost"
//    ).show()

    //终端设备
    //运营
//    logMark.groupBy("ispname").agg(
    ////      sum("srcReq") as "srcReqNum",
    ////      sum("validReq") as "validReqNum",
    ////      sum("adReq") as "adReqNum",
    ////      sum("bidding") as "biddingNum",
    ////      sum("win") as "winNum",
    ////      sum("win")/sum("bidding")*1.0 as "winRate",
    ////      sum("showReq") as "showReqNum",
    ////      sum("clickReq") as "clickReqNum",
    ////      sum("clickReq")/sum("showReq") as "clickRate",
    ////      sum("DSPAdCons")/1000 as "DSPAdConsume",
    ////      sum("DSPAdCo")/1000 as "DSPAdCost"
    ////    ).show()

    //网络类
//    logMark.groupBy("networkmannername").agg(
//      sum("srcReq") as "srcReqNum",
//      sum("validReq") as "validReqNum",
//      sum("adReq") as "adReqNum",
//      sum("bidding") as "biddingNum",
//      sum("win") as "winNum",
//      sum("win")/sum("bidding")*1.0 as "winRate",
//      sum("showReq") as "showReqNum",
//      sum("clickReq") as "clickReqNum",
//      sum("clickReq")/sum("showReq") as "clickRate",
//      sum("DSPAdCons")/1000 as "DSPAdConsume",
//      sum("DSPAdCo")/1000 as "DSPAdCost"
//    ).show()

    //设备类
//    logMark.groupBy("devicetype").agg(
//      sum("srcReq") as "srcReqNum",
//      sum("validReq") as "validReqNum",
//      sum("adReq") as "adReqNum",
//      sum("bidding") as "biddingNum",
//      sum("win") as "winNum",
//      sum("win")/sum("bidding")*1.0 as "winRate",
//      sum("showReq") as "showReqNum",
//      sum("clickReq") as "clickReqNum",
//      sum("clickReq")/sum("showReq") as "clickRate",
//      sum("DSPAdCons")/1000 as "DSPAdConsume",
//      sum("DSPAdCo")/1000 as "DSPAdCost"
//    ).show()

    //操作系
//    logMark.groupBy("client").agg(
//      sum("srcReq") as "srcReqNum",
//      sum("validReq") as "validReqNum",
//      sum("adReq") as "adReqNum",
//      sum("bidding") as "biddingNum",
//      sum("win") as "winNum",
//      sum("win")/sum("bidding")*1.0 as "winRate",
//      sum("showReq") as "showReqNum",
//      sum("clickReq") as "clickReqNum",
//      sum("clickReq")/sum("showReq") as "clickRate",
//      sum("DSPAdCons")/1000 as "DSPAdConsume",
//      sum("DSPAdCo")/1000 as "DSPAdCost"
//    ).show()

    //媒体分析
    val appRDD: RDD[String] = sparkSession.sparkContext.textFile("E:\\DMPData\\input\\app_dict.txt")

    val res = appRDD.filter(x =>x.split("\t",-1).length>=5).map(x => {
      val fields: Array[String] = x.split("\t",-1)
      (fields(4),fields(1))
    }).collect()
    val map: Map[String, String] = res.toMap

    val broadcastVar: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)

    logMark.groupBy(when($"appname"==="其他",broadcastVar.value.getOrElse($"appid".toString(),$"appid".toString())).otherwise($"appname") as "appName").agg(
      sum("srcReq") as "srcReqNum",
      sum("validReq") as "validReqNum",
      sum("adReq") as "adReqNum",
      sum("bidding") as "biddingNum",
      sum("win") as "winNum",
      sum("win")/sum("bidding")*1.0 as "winRate",
      sum("showReq") as "showReqNum",
      sum("clickReq") as "clickReqNum",
      sum("clickReq")/sum("showReq") as "clickRate",
      sum("DSPAdCons")/1000 as "DSPAdConsume",
      sum("DSPAdCo")/1000 as "DSPAdCost"
    ).show()

    //渠道报表
    logMark.groupBy("adplatformproviderid").agg(
      sum("srcReq") as "srcReqNum",
      sum("validReq") as "validReqNum",
      sum("adReq") as "adReqNum",
      sum("bidding") as "biddingNum",
      sum("win") as "winNum",
      sum("win")/sum("bidding")*1.0 as "winRate",
      sum("showReq") as "showReqNum",
      sum("clickReq") as "clickReqNum",
      sum("clickReq")/sum("showReq") as "clickRate",
      sum("DSPAdCons")/1000 as "DSPAdConsume",
      sum("DSPAdCo")/1000 as "DSPAdCost"
    ).show()

    sparkSession.stop()












  }

}
