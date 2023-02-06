package cn.spark.broadCast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkBroadCast {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkBroadCast").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    sparkContext.setLogLevel("warn")

    //读取文件，将商品文件数据内容作为广播变量
    //样本数据 p0001,xiaomi,1000,2
    val productRdd: RDD[String] = sparkContext.textFile("/Users/bruce/Desktop/SparkPro/spark_day02/src/main/resources/pdts.txt")
    //将数据收集起来
    val mapProduct: collection.Map[String, String] = productRdd.map(x => {
      (x.split(",")(0), x)
    }).collectAsMap()
    //开始广播
    val broadCastValue: Broadcast[collection.Map[String, String]] = sparkContext.broadcast(mapProduct)

    //读取订单数据
    val ordersRDD: RDD[String] = sparkContext.textFile("/Users/bruce/Desktop/SparkPro/spark_day02/src/main/resources/orders.txt")
    //订单数据rdd进行拼接商品数据
    val proudctAndOrderRdd: RDD[String] = ordersRDD.mapPartitions(eachPartition => {
      //获得广播变量的值
      val getBroadCastMap: collection.Map[String, String] = broadCastValue.value

      val finalStr: Iterator[String] = eachPartition.map(eachLine => {
        //eachLine 样本数据 1001,20150710,p0001,2
        val ordersGet: Array[String] = eachLine.split(",")
        val getProductStr: String = getBroadCastMap.getOrElse(ordersGet(2), "")
        eachLine + "\t" + getProductStr
      })
      finalStr
    })
    //    println(proudctAndOrderRdd.collect().toBuffer)
    for(item <- proudctAndOrderRdd.collect()) {
      println(item)
    }

    sparkContext.stop()
  }
}