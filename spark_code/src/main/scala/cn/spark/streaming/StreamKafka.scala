package cn.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util
import scala.collection.mutable

object StreamKafka {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    // todo: 1、创建SparkConf对象  注意这里最少给两个线程也就是local[2]  一个县城没法执行，为啥？？？
    val sparkConf: SparkConf = new SparkConf().setAppName("TcpWordCount").setMaster("local[2]")
    // todo: 2、创建StreamingContext对象  Seconds(1)  表示每隔多长时间去处理一次数据
    val ssc = new StreamingContext(sparkConf,Seconds(1))


    /**
     *   ssc: StreamingContext,
      *locationStrategy: LocationStrategy,
      *consumerStrategy: ConsumerStrategy[K, V]
     */

      //kafka消费的offset默认是保存在kafka的一个topic里面了__consumer_offsets
   /* val partitionToLong = new mutable.HashMap[TopicPartition, Long]()
    val partition = new TopicPartition("test", 1)*/

      val topic = Set("test")
    val kafkaParams = Map(
      "bootstrap.servers" -> "bigdata01:9092,bigdata01:9092,bigdata01:9092",
      "group.id" -> "KafkaDirect10",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"
    )

    //定义数据消费的使用策略
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent

    //定义从哪里开始进行消费
    /**
     *  topics: ju.Collection[jl.String],
      kafkaParams: ju.Map[String, Object],
      offsets: ju.Map[TopicPartition, jl.Long]  //表示我们消费到了哪一个topic的哪一个分区的对应的offset的值
     */
  /*  val map = new util.HashMap[TopicPartition,Long]
    val partition = new TopicPartition("test", 1)
    map.put(partition,4000L)
*/
    val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe[String, String](topic, kafkaParams)
    //使用工具类直接获取kafka的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy, consumerStrategy)

    kafkaDStream.foreachRDD(rdd =>{
      //处理数据
      val dataRDD: RDD[String] = rdd.map(_.value())
      dataRDD.foreach(line =>{
        println(line)
      })

      //一个rdd的数据处理完了，需要提交offset的值
      val offsetRange: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for(eachRange <- offsetRange){
        val startOffset: Long = eachRange.fromOffset
        val endOffset: Long = eachRange.untilOffset
        val topicName: String = eachRange.topic
        val partitioName: Int = eachRange.partition
        println(startOffset)
        println(endOffset)
        println(topicName)
        println(partitioName)
      }
      //将offset给提交到kafka里面去保存 手动提交到kafka的默认的topic里面去保存了
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRange)
    })


    ssc.start()
    ssc.awaitTermination()


  }


}
