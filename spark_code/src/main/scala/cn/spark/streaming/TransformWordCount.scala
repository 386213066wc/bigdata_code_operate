package cn.spark.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 * 获取每一个批次中单词出现次数最多的前3位
 */
object TransformWordCount {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("TransformWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01",9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    //todo: 5、将Dstream进行transform方法操作
    //通过transform的操作，主要是将DStream转换成为RDD的操作，可以使用RDD的所有的算子
    val sortedDstream: DStream[(String, Int)] = result.transform(rdd => {
      //对单词出现的次数进行排序
      val sortedRDD: RDD[(String, Int)] = rdd.sortBy(_._2, false)

      val top3: Array[(String, Int)] = sortedRDD.take(3)
      println("------------top3----------start")
      top3.foreach(println)
      println("------------top3------------end")
      sortedRDD
    })

    //todo: 6、打印该批次中所有单词按照次数降序的结果
    sortedDstream.print()



    //todo: 7、开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }
}