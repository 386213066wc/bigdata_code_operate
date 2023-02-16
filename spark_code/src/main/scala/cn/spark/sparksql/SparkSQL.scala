package cn.spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

//定义一个样例类
//case class Person(id: String, name: String, age: Int)

object SparkSQL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkDSL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    //加载数据
    val rdd1 = sc.textFile("file:///E:\\1、课程内容\\1、课程资料\\bigdata_code_operate\\spark_code\\src\\main\\resources\\person.txt").map(x => x.split(" "))

    //把rdd与样例类进行关联
    val personRDD = rdd1.map(x => Person(x(0), x(1), x(2).toInt))
    //把rdd转换成DataFrame
    import spark.implicits._ // 隐式转换

    val personDF = personRDD.toDF
    //打印schema信息
    personDF.printSchema
    //展示数据
    personDF.show

    //DataFrame注册成表
    personDF.createTempView("person")

    //使用SparkSession调用sql方法统计查询
    spark.sql("select * from person").show
    spark.sql("select name from person").show
    spark.sql("select name,age from person").show
    spark.sql("select * from person where age >30").show
    spark.sql("select count(*) from person where age >30").show
    spark.sql("select age,count(*) from person group by age").show
    spark.sql("select age,count(*) as count from person group by age").show
    spark.sql("select * from person order by age desc").show

    spark.stop()
  }
}