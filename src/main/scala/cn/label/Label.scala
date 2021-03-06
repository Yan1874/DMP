package cn.label

import cn.util.{HbaseUtil, LabelUtil, UserIdUtil, mapUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
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
    }).collect().toMap
    val broadcastVar = sparkSession.sparkContext.broadcast(res)

    //停词广播变量
    val appRDD1: RDD[String] = sparkSession.sparkContext.textFile("E:\\DMPData\\input\\app_dict.txt")
    val stop = appRDD1.map((_,1)).collect()
    val stopWords = sparkSession.sparkContext.broadcast(stop.toMap)


    val res1 = df.rdd.map(row => {
      val userid = UserIdUtil.getUserId(row)
      val map = new mutable.HashMap[String,Int]()

      LabelUtil.adspacetypeLb(row,map)
      LabelUtil.adspacetypenameLb(row,map)
      LabelUtil.appnameLb(row,broadcastVar,map)
      LabelUtil.adplatformprovideridLb(row,map)
      LabelUtil.clientLb(row,map)
      LabelUtil.networkmannernameLb(row,map)
      LabelUtil.ispnameLb(row,map)
      LabelUtil.keywordsLb(row,stopWords,map)
      LabelUtil.pricityLb(row,map)
      LabelUtil.businessLb(row,map)
      (userid,map)
  }).groupByKey().mapValues(x => {
      x.reduce((x,y)=>{
        mapUtil.mergeMap(x,y)
      })
  }).map{
      case (userId,userTags) => {
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("2019-09-22"),Bytes.toBytes(userTags.mkString(",")))

        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(HbaseUtil.getConf(sparkSession))

    sparkSession.stop()


  }
}
