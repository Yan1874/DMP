package cn.location

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCt {
  def main(args: Array[String]): Unit = {

    if(args.length != 1) {
      sys.exit()
    }

    val Array(inputPath) = args

    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("CT")
      .master("local[*]")
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val df: DataFrame = sparkSession.read.parquet(inputPath)
    import org.apache.spark.sql.functions._
    val res: DataFrame = df.select("provinceName","cityName").groupBy("provinceName","cityName").agg(count("*"))

//    res.write.partitionBy("provinceName","cityName").json("E:\\out")

    res.write.format("jdbc")
      .mode("append")
            .option("url","jdbc:mysql://hadoop0001:3306/bigdata?useUnicode=true&amp;characterEncoding=UTF-8")
            .option("user","root")
            .option("password","123456")
            .option("dbtable","procity")
            .save()
    sparkSession.stop()
  }
}
