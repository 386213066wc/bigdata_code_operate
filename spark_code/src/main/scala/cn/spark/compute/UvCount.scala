package cn.spark.compute

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//求出所有ip地址，再去重
object UvCount {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("warn")

    //3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("access.log").getPath)

    //4、获取所有的ip地址
    val ipsRDD: RDD[String] = dataRDD.map(x => x.split(" ")(0))

    //5、对ip地址进行去重
    val distinctRDD: RDD[String] = ipsRDD.distinct()

    //6、统计uv
    val uv: Long = distinctRDD.count()
    println(s"uv:$uv")
    sc.stop()
  }
}