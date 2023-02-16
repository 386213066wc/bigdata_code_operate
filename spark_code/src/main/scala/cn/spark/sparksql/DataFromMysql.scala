package cn.spark.sparksql

import java.util.Properties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//todo:利用sparksql加载mysql表中的数据
object DataFromMysql {

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DataFromMysql").setMaster("local[2]")
    //2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //3、读取mysql表的数据
    //3.1 指定mysql连接地址
    val url="jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8"
    //3.2 指定要加载的表名
    val tableName="my_user"
    // 3.3 配置连接数据库的相关属性
    val properties = new Properties()
    //用户名
    properties.setProperty("user","root")
    //密码
    properties.setProperty("password","123456")
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)
    //打印schema信息
    mysqlDF.printSchema()
    //展示数据
    mysqlDF.show()
    //把dataFrame注册成表
    mysqlDF.createTempView("job_detail")
    spark.sql("select * from job_detail ").show()
    spark.stop()
  }
}