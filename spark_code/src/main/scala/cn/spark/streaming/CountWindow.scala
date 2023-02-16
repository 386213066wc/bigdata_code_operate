package cn.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CountWindow {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("MapWithStateWordCount").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01", 9999)

    val words: DStream[String] = lines.flatMap(x => x.split(" "))
    val pairs: DStream[(String, Int)] = words.map(word => (word, 1))

    val wordCounts: DStream[(String, Int)] = pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(12), Seconds(12))

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()







  }


}
