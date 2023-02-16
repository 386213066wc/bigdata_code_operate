package cn.spark.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 通过checkpoint来恢复Driver端
 */
object DriverHAWordCount {

  val checkpointPath="hdfs://bigdata01:8020/checkpointDir"

  def creatingFunc(): StreamingContext ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("DriverHAWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //设置checkpoint目录
    ssc.checkpoint(checkpointPath)

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("node01",9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateFunc)

    //todo: 5、打印结果
    result.print()

    ssc
  }

  //currentValue:当前批次中每一个单词出现的所有的1
  //historyValues:之前批次中每个单词出现的总次数,Option类型表示存在或者不存在。 Some表示存在有值，None表示没有
  def updateFunc(currentValue:Seq[Int], historyValues:Option[Int]):Option[Int] = {

    val newValue: Int = currentValue.sum + historyValues.getOrElse(0)
    Some(newValue)
  }


  def main(args: Array[String]): Unit = {

    //从checkpoint里面恢复上一次记录的streaming程序的Drvier端

    val ssc: StreamingContext = StreamingContext.getOrCreate(checkpointPath,creatingFunc _)

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }
}
