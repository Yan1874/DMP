package cn.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TestDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    val jsonRDD: RDD[String] = sparkSession.sparkContext.textFile("C:\\Users\\YAN\\Desktop\\考试\\9-22\\json.txt")

    val res = jsonRDD.map(line => {
      val businessarea: String = Util.getBusinessarea(line)
      businessarea
    }).flatMap(_.split(",")).filter(x => x!="[]").map((_,1)).reduceByKey(_+_).collect().toBuffer

    //1、按照pois，分类businessarea，并统计每个businessarea的总数。
    //ArrayBuffer((白杨,30), (下沙,30), (九堡街道,30), (洋泾,30), (新天地(自忠路),30), (太平桥,30), (城北,30), (站北,30))
    println(res)

    val res1 =jsonRDD.map(line => {
      val types: String = Util.getType(line)
      types
    }).flatMap(_.split(",")).flatMap(_.split(";")).map((_,1)).reduceByKey(_+_).collect().toBuffer

    //2、按照pois，分类type，为每一个Type类型打上标签，统计各标签的数量
    //ArrayBuffer((医疗保健服务场所,2), (快餐厅,7), (社会治安机构,2), (公园广场,9), (火车站,16), (外国餐厅,3), (传媒机构,4), (政府机关,84), (中餐厅,2), (三级甲等医院,2), (大中,1), (图书馆,2), (口腔医院,2), (产业园区,8), (培训机构,32), (广发银行,1), (交通银行,3), (网络科技,1), (麦当劳,1), (省直辖市级政府及事业单位,8), (区县级政府及事业单位,48), (政府机构及社会团体,92), (家电电子卖场,1), (购物中心,2), (肯德基,1), (五星级宾馆,2), (金融保险服务,25), (银行,30), (卫生院,1), (中国农业银行,2), (住宿服务,26), (超级市场,1), (地铁站,10), (风景名胜,13), (幼儿园,7), (西餐厅(综合风味),2), (公安警察,4), (公司,26), (宿舍,3), (经济型连锁酒店,3), (餐饮服务,10), (成人教育|科教文化服务,1), (购物服务,9), (商住两用楼宇,3), (退票,1), (商务住宅相关,4), (公检法机关,1), (文艺团体,6), (文化宫,4), (普通商场,3), (中国建设银行,3), (街道级地名,1), (中国银行,1), (出站口,1), (高等院校,1), (医疗保健服务,1
    println(res1)














  }
}
