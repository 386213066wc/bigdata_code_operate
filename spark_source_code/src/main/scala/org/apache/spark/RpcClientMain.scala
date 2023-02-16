package org.apache.spark


import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcEnv}
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 *
 *  注释：客户端
 */
object RpcClientMain {

  def main(args: Array[String]): Unit = {

    /**
     *
     *  注释： 获取 SparkEnv
     */
    val conf: SparkConf = new SparkConf()
    val sparkSession = SparkSession.builder().config(conf).master("local[*]").appName("test rpc").getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext
    val sparkEnv: SparkEnv = sparkContext.env

    /**
     */

    val rpcEnv: RpcEnv = RpcEnv
      .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf, sparkEnv.securityManager, false)

    /**
     * 获取RpcEndpoint的Ref引用对象
     */
    val endPointRef: RpcEndpointRef = rpcEnv
      .setupEndpointRef(RpcAddress(HelloRpcSettings.getHostname(), HelloRpcSettings.getPort()), HelloRpcSettings.getName())

    import scala.concurrent.ExecutionContext.Implicits.global

    // 注释： one way 异步发送，发送过后既忘
    // TODO_MA_Z 重要注释:  发消息： 发送消息给对方，不需要返回  服务端会由 receive方法来处理
    endPointRef.send(SayHi("test send"))


    // 注释： 不重试的异步发送一次消息， 对应的 endpoint组件通过 receiveAndReply 进行处理
    // 注释： 在超时时间内，返回一个 Future 对象
    // TODO_MA_Z 重要注释:  发消息， 异步发送，获取到 Future  客户端调用ask方法，服务端会由receiveAndReply方法来处理
    val future: Future[String] = endPointRef.ask[String](SayHi("test ask"))
    future.onComplete { case scala.util.Success(value) => println(s"Got the Ask result = $value")
    case scala.util.Failure(e) => println(s"Got the Ask error: $e")
    }
    Await.result(future, Duration.apply("30s"))

    // 注释： 同步发送消息，等待响应，可能等待超时
    // TODO_MA_Z 重要注释:  发消息， 异步发送消息，同步等待结果
    val res = endPointRef.askSync[String](SayBye("test askSync"))
    println(s"Got the AskSync result = $res")
    rpcEnv.awaitTermination()
  }
}
