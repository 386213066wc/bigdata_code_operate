package cn.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingCode {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    // todo: 1、创建SparkConf对象  注意这里最少给两个线程也就是local[2]  一个县城没法执行，为啥？？？
    val sparkConf: SparkConf = new SparkConf().setAppName("TcpWordCount").setMaster("local[2]")
    // todo: 2、创建StreamingContext对象  Seconds(1)  表示每隔多长时间去处理一次数据
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    val lineStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)
    lineStream.flatMap(x => x.split(" ")).map((_,1)).reduceByKey(_ + _).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
