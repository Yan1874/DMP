package cn.util

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableDescriptors, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object HbaseUtil {
  def getConf(sparkSession: SparkSession):JobConf={
    val hbaseTableName: String = ConfigFactory.load().getString("HBASE.tableName")
    val hbaseHost: String = ConfigFactory.load().getString("HBASE.host")

    //创建Hadoop任务
    val configuration: Configuration = sparkSession.sparkContext.hadoopConfiguration
    //配置Hbase连接
    configuration.set("hbase.zookeeper.quorum",hbaseHost)
    //获取connection连接
    val connection: Connection = ConnectionFactory.createConnection(configuration)
    //获取admin
    val admin: Admin = connection.getAdmin
    if(!admin.tableExists(TableName.valueOf(hbaseTableName))) {
      //创建表对象
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      //创建列簇
      val hColumnDescriptor = new HColumnDescriptor("tags")
      //将列簇添加到表中
      tableDescriptor.addFamily(hColumnDescriptor)
      //建表
      admin.createTable(tableDescriptor)
      admin.close()
      connection.close()
    }
    val conf = new JobConf(configuration)
    conf.setOutputFormat(classOf[TableOutputFormat])
    conf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
    conf

  }

}
