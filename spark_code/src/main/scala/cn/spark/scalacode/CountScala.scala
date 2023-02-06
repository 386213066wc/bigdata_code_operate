package cn.spark.scalacode

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountScala {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("countScala")

    val context = new SparkContext(sparkConf)

    val result: Array[(String, Int)] = context.textFile("hdfs://bigdata01:8020/count.txt").flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((x, y) => x + y)
      .collect()  //action算子 每一个action算子，就是一个job

    ///没有去执行action算子，就会报错
    context.textFile("hdfs://bigdata01:8020/count.txt").flatMap(line => line.split(" "))
      .map(word => (word, 1)).foreach(print)  //action算子

    result.foreach(println)

    context.stop()


  }


}
