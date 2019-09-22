package cn.label

import cn.util.{LabelUtil, mapUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Label {
  def main(args: Array[String]): Unit = {
    if(args.length != 1) {
      println("输入目录错误")
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession = SparkSession
      .builder()
      .appName("APP")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = sparkSession.read.parquet(inputPath)

    //广播变量
    val appRDD: RDD[String] = sparkSession.sparkContext.textFile("E:\\DMPData\\input\\app_dict.txt")
    val res = appRDD.filter(x =>x.split("\t",-1).length>=5).map(x => {
      val fields: Array[String] = x.split("\t",-1)
      (fields(4),fields(1))
    }).collect()
    val map: Map[String, String] = res.toMap
    val broadcastVar: Broadcast[Map[String, String]] = sparkSession.sparkContext.broadcast(map)


    val res1 = df.rdd.filter(row =>StringUtils.isNotBlank(row.getAs[String]("userid"))).map(row => {
      val userid = row.getAs[String]("userid")
      val adspacetype = row.getAs[Int]("adspacetype")
      val adspacetypename = row.getAs[String]("adspacetypename")
      val appid = row.getAs[String]("appid")
      val appname = row.getAs[String]("appname")
      val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
      val client = row.getAs[Int]("client")
      val networkmannername = row.getAs[String]("networkmannername")
      val ispname = row.getAs[String]("ispname")
      val keywords = row.getAs[String]("keywords")
      val provincename = row.getAs[String]("provincename")
      val cityname = row.getAs[String]("cityname")
      val long = row.getAs[String]("long")
      val lat = row.getAs[String]("lat")
      val map = new mutable.HashMap[String,Int]()

      LabelUtil.adspacetypeLb(adspacetype,map)
      LabelUtil.adspacetypenameLb(adspacetypename,map)
      LabelUtil.appnameLb(appid,appname,broadcastVar,map)
      LabelUtil.adplatformprovideridLb(adplatformproviderid,map)
      LabelUtil.clientLb(client,map)
      LabelUtil.networkmannernameLb(networkmannername,map)
      LabelUtil.ispnameLb(ispname,map)
      LabelUtil.keywordsLb(keywords,map)
      LabelUtil.pricityLb(provincename,cityname,map)
      LabelUtil.businessLb(long,lat,map)
      (userid,map)
  }).groupByKey().mapValues(x => {
      x.reduce((x,y)=>{
        mapUtil.mergeMap(x,y)
      })
  }).collect()

    println(res1.toBuffer)



    sparkSession.stop()


  }
}
