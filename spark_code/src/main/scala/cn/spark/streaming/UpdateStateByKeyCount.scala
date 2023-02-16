package cn.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyCount {

  //将历史的统计结果状态给保存下来，进行累加
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // todo: 1、创建SparkConf对象  注意这里最少给两个线程也就是local[2]  一个县城没法执行，为啥？？？
    val sparkConf: SparkConf = new SparkConf().setAppName("TcpWordCount").setMaster("local[2]")
    // todo: 2、创建StreamingContext对象  Seconds(1)  表示每隔多长时间去处理一次数据
    val ssc = new StreamingContext(sparkConf,Seconds(1))
    //设置ck路径，用于保存状态
    ssc.checkpoint("./check_point")

    val sockeTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)

    val wordAndOne: DStream[(String, Int)] = sockeTextStream.flatMap(_.split(" ")).map(x => (x, 1))

    val result: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc)

    result.print()

    ssc.start()
    ssc.awaitTermination()





  }

  //定时历史状态累加函数
  def updateFunc(currentValue:Seq[Int],historyValue:Option[Int]):Option[Int]={
    val newValue: Int = currentValue.sum + historyValue.getOrElse(0)

    Some(newValue)


  }



}
