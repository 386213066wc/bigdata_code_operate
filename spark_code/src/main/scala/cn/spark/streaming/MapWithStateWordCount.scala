package cn.spark.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream, ReceiverInputDStream}
import org.apache.spark.streaming._

/**
 * mapWithState实现把所有批次的单词出现的次数累加
 * --性能更好
 */
object MapWithStateWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("MapWithStateWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //从集合当中获取一个RDD
    val initRDD: RDD[(String, Int)] = ssc.sparkContext.parallelize((List(("hadoop",10),("spark",20))))

    //需要设置checkpoint目录，用于保存之前批次的结果数据,该目录一般指向hdfs路径
    ssc.checkpoint("./ck_state")

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01",9999)

    //todo: 4、对数据进行处理
    val wordAndOneDstream: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1))


    val stateSpec=StateSpec.function((time:Time,key:String,currentValue:Option[Int],historyState:State[Int])=>{

      //当前批次结果与历史批次的结果累加
      val sumValue: Int = currentValue.getOrElse(0)+ historyState.getOption().getOrElse(0)
      val output=(key,sumValue)


      if(!historyState.isTimingOut()){
        historyState.update(sumValue)
      }

      Some(output)
      //给一个初始的结果initRDD
      //timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉
    }).initialState(initRDD).timeout(Durations.seconds(5))


    //使用mapWithState方法，实现累加
    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = wordAndOneDstream.mapWithState(stateSpec)

    //todo: 5、打印所有批次的结果数据
    result.stateSnapshots().print()

    //todo: 6、开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}