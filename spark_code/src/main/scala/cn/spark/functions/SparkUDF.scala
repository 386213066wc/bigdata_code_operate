package cn.spark.functions

import org.apache.spark.SparkConf
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.regex.{Matcher, Pattern}

object SparkUDF {
  def main(args: Array[String]): Unit = {

    //获取sparkSession对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    //读取CSV文件
    val frame: DataFrame = session.read.format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("E:\\1、课程内容\\1、课程资料\\9、Spark一站式解决方案\\2、SparkSQL结构化数据处理模块\\2、数据资料\\数据资料\\深圳链家二手房成交明细\\深圳链家二手房成交明细.csv")

    frame.createOrReplaceTempView("house_sale")
    //定义函数处理数据


    //自定义函数
    session.udf.register("house_udf",new UDF1[String,String] {
      private val pattern: Pattern = Pattern.compile("^[0-9]*$")

      override def call(input: String): String = {
        val matcher: Matcher = pattern.matcher(input)
        if(matcher.matches()){
          input
        }else{
          "1990"
        }
      }
    },DataTypes.StringType)


    session.sql("select house_udf(house_age) from house_sale limit 100").show()
    session.stop()



  }

}
