package cn.label

import cn.util.{HbaseUtil, LabelUtil, UserIdUtil, mapUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object Label2 {
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

    val userRDD = df.rdd.map(row => {
      //获取所有标识用户的信息
      val userList: List[String] = UserIdUtil.getAllUserId(row)
      (userList,row)
    })

    val verties = userRDD.flatMap(x => {
      val row = x._2

      val map = new mutable.HashMap[String,Int]()
      //打标签并将标签put到一个map中
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

      //将所有用户信息和标签放入一个List
      val VD: List[(String, Int)] = x._1.map((_,0))++map

      //取用户的第一个信息和VD(包含全部用户信息和标签)组成元组
      x._1.map(uId => {
        if(x._1.head.equals(uId)) {
          (uId.hashCode.toLong,VD)
        }else {
          (uId.hashCode.toLong,List.empty)
        }
      })
    })

    //将用户第一个信息和其他每条的信息构建边的集合
    val edges = userRDD.flatMap(x => {
      x._1.map(uId => Edge(x._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    //构建图
    val graph = Graph(verties,edges)
    //连接所有点并找到最小点作为公共点
    val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices

    //匹配数据
    vertices.join(verties).map {
      //（用户标识，（顶点，标签））
      case (uId, (cnId, tages)) => {
        (cnId,tages)
      }
    }.map{

      case (userId,userTags) => {
        val put = new Put(Bytes.toBytes(userId))
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("2019-09-22"),Bytes.toBytes(userTags.mkString(",")))

        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(HbaseUtil.getConf(sparkSession))

    sparkSession.stop()


  }
}
