package cn.spark.streaming

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import scalikejdbc.{ConnectionPool, DB, _}

/**
 * SparkStreaming EOS:
 * Input:Kafka
 * Process:Spark Streaming
 * Output:Mysql
 *
 * mysql支持事务操作：
 * （）
 *
 * 保证EOS:
 * 1、偏移量自己管理，即enable.auto.commit=false,这里保存在Mysql中
 * 2、使用createDirectStream
 * 3、事务输出: 结果存储与Offset提交在Driver端同一Mysql事务中
 */
object SparkStreamingEOSKafkaMysqlAtomic {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val topic = "topic1"
    val group = "spark_app1"

    //Kafka配置
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:6667,node2:6667,node3:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "group.id" -> group)

    //在Driver端创建数据库连接池
    ConnectionPool.singleton("jdbc:mysql://node3:3306/bigdata", "", "")

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.replace("$", ""))
    val ssc = new StreamingContext(conf, Seconds(5))

    //1)初次启动或重启时,从指定的Partition、Offset构建TopicPartition
    //2)运行过程中,每个Partition、Offset保存在内部currentOffsets = Map[TopicPartition, Long]()变量中
    //3)后期Kafka Topic分区动扩展,在运行过程中不能自动感知
    val initOffset = DB.readOnly(implicit session => {
      sql"select `partition`,offset from kafka_topic_offset where topic =${topic} and `group`=${group}"
        .map(item => new TopicPartition(topic, item.get[Int]("partition")) -> item.get[Long]("offset"))
        .list().apply().toMap
    })

    //CreateDirectStream
    //从指定的Topic、Partition、Offset开始消费
    val sourceDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](initOffset.keys, kafkaParams, initOffset)
    )

    sourceDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(offsetRange => {
          logger.info(s"Topic: ${offsetRange.topic},Group: ${group},Partition: ${offsetRange.partition},fromOffset: ${offsetRange.fromOffset},untilOffset: ${offsetRange.untilOffset}")
        })
        //统计分析
        //将结果收集到Driver端
        val sparkSession = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import sparkSession.implicits._
        val dataFrame = sparkSession.read.json(rdd.map(_.value()).toDS)
        dataFrame.createOrReplaceTempView("tmpTable")
        val result = sparkSession.sql(
          """
            |select
            |   --每分钟
            |   eventTimeMinute,
            |   --每种语言
            |   language,
            |   -- 次数
            |   count(1) pv,
            |   -- 人数
            |   count(distinct(userID)) uv
            |from(
            |   select *, substr(eventTime,0,16) eventTimeMinute from tmpTable
            |) as tmp group by eventTimeMinute,language
          """.stripMargin
        ).collect()

        //在Driver端存储数据、提交Offset
        //结果存储与Offset提交在同一事务中原子执行
        //这里将偏移量保存在Mysql中
        DB.localTx(implicit session => {

          //结果存储
          result.foreach(row => {
            sql"""
            insert into twitter_pv_uv (eventTimeMinute, language,pv,uv)
            value (
                ${row.getAs[String]("eventTimeMinute")},
                ${row.getAs[String]("language")},
                ${row.getAs[Long]("pv")},
                ${row.getAs[Long]("uv")}
                )
            on duplicate key update pv=pv,uv=uv
          """.update.apply()
          })

          //Offset提交
          offsetRanges.foreach(offsetRange => {
            val affectedRows =
              sql"""
          update kafka_topic_offset set offset = ${offsetRange.untilOffset}
          where
            topic = ${topic}
            and `group` = ${group}
            and `partition` = ${offsetRange.partition}
            and offset = ${offsetRange.fromOffset}
          """.update.apply()

            if (affectedRows != 1) {
              throw new Exception(s"""Commit Kafka Topic: ${topic} Offset Faild!""")
            }
          })
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}