package cn.spark.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PvCount {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")
    //3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("access.log").getPath)
    //4、统计PV
    val pv: Long = dataRDD.count()
    println(s"pv:$pv")
    //5、关闭sc
    sc.stop()
  }
}