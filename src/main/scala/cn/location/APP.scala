package cn.location

import cn.util.RPTUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

object APP {
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
    val res = df.rdd.map(row => {
      val provinceName = row.getAs[String]("provincename")
      val cityName = row.getAs[String]("cityname")
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val winprice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")

      val t1: (Int, Int, Int) = RPTUtil.reqPt(requestmode,processnode)
      val t2: (Int, Int) = RPTUtil.showClick(requestmode,iseffective)
      val t3: (Int, Int, Double, Double) = RPTUtil.adRTB(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)

      ((provinceName,cityName),(t1._1,t1._2,t1._3,t2._1,t2._2,t3._1,t3._2,t3._3,t3._4))
    }).reduceByKey((x,y) =>{
      (x._1+y._1,x._2+y._2,x._3+y._3,x._4+y._4,x._5+y._5,x._6+y._6,x._7+y._7,x._8+y._8,x._9+y._9)
    }).collect.toBuffer

    println(res)
























  }
}
