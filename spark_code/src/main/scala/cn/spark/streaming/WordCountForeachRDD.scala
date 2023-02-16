package cn.spark.streaming


import java.sql.DriverManager
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
 *  将WordCount案例中得到的结果通过foreachRDD保存结果到mysql中
 */
object WordCountForeachRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    //现在connection是在Driver端进行初始化了
  //  val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")


    // todo: 1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCountForeachRDD").setMaster("local[2]")

    // todo: 2、创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf,Seconds(2))

    //todo: 3、接受socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("bigdata01",9999)

    //todo: 4、对数据进行处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    //todo: 5、保存结果到mysql表中

    //todo:方案一（有问题）

    //算子是在exuecotr端执行的  这种方式使用，也会有序列化的问题
    result.foreachRDD(rdd =>{
      //注意这里创建的对象都是在Driver端
      val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
      val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")
      rdd.foreach { record =>
        //这一块代码的执行是在executor端，需要进行网络传输，会出现task not serializable 异常
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()
      }
      statement.close()
      conn.close()

    })

    //todo: 方案二
    result.foreachRDD(rdd=>{
      //遍历
      rdd.foreach { record =>

        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()

        statement.close()
        conn.close()
      }
    })


    //todo: 方案三
    result.foreachRDD(rdd=>{
      rdd.foreachPartition( iter =>{
        //这里是个每个partition数据创建一个链接对象，这样子会不会有序列化的问题？？？
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")

        iter.foreach( record =>{
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          statement.execute()

        })
        statement.close()
        conn.close()
      })
    })

    //todo: 方案四
    result.foreachRDD(rdd=>{
      rdd.foreachPartition( iter =>{
        val conn = DriverManager.getConnection("jdbc:mysql://node03:3306/test", "root", "123456")
        val statement = conn.prepareStatement(s"insert into wordcount(word, count) values (?, ?)")
        //关闭自动提交
        conn.setAutoCommit(false);
        iter.foreach( record =>{
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          //添加到一个批次
          statement.addBatch()

        })
        //批量提交该分区所有数据
        statement.executeBatch()
        conn.commit()
        // 关闭资源
        statement.close()
        conn.close()
      })
    })


    //todo: 6、开启流式计算
    ssc.start()
    ssc.awaitTermination()

  }
}