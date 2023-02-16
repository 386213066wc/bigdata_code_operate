package cn.spark.count

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

//用sparksql加载mysql表中的数据
object DataFromMysqlPlan {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DataFromMysql").setMaster("local[2]")

    //sparkConf.set("spark.sql.codegen.wholeStage","true")
    //2、创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //3、读取mysql表的数据
    //3.1 指定mysql连接地址
    val url="jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8"
    //3.2 指定要加载的表名
    val student="students"
    val score="scores"

    // 3.3 配置连接数据库的相关属性
    val properties = new Properties()

    //用户名
    properties.setProperty("user","root")
    //密码
    properties.setProperty("password","123456")

    val studentFrame: DataFrame = spark.read.jdbc(url,student,properties)
    val scoreFrame: DataFrame = spark.read.jdbc(url,score,properties)
    //把dataFrame注册成表
    studentFrame.createTempView("students")
    scoreFrame.createOrReplaceTempView("scores")
    val resultFrame: DataFrame = spark.sql("SELECT temp1.class,SUM(temp1.degree),AVG(temp1.degree)  FROM (SELECT  students.sno AS ssno,students.sname,students.ssex,students.sbirthday,students.class, scores.sno,scores.degree,scores.cno  FROM students LEFT JOIN scores ON students.sno =  scores.sno  WHERE degree > 60 AND sbirthday > '1973-01-01 00:00:00' ) temp1 GROUP BY temp1.class")
    resultFrame.explain(true)
    resultFrame.show()
    spark.stop()
  }
}