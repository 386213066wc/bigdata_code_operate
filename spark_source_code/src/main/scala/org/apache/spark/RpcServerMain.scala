package org.apache.spark


import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}
import org.apache.spark.sql.SparkSession

/**
 *
 *  注释： Spark RPC 服务端
 */
object RpcServerMain {

  def main(args: Array[String]): Unit = {

    /**
     *
     *  注释： 获取 SparkEnv
     */
    val conf: SparkConf = new SparkConf()
    val sparkSession = SparkSession.builder().config(conf).master("local[*]").appName("sparkRPC").getOrCreate()
    val sparkContext: SparkContext = sparkSession.sparkContext

    val sparkEnv: SparkEnv = sparkContext.env

    /**
     *
     *  注释： 构建 RpcEnv， 当做 Akka 的 ActorySystem 去看待
     */
    val rpcEnv = RpcEnv
      .create(HelloRpcSettings.getName(), HelloRpcSettings.getHostname(), HelloRpcSettings.getHostname(), HelloRpcSettings.getPort(), conf,
        sparkEnv.securityManager, 1, false)

    // 注释：创建 和启动 endpoint， 当做 Akka 的 actor 来看待
    val helloEndpoint: RpcEndpoint = new HelloEndPoint(rpcEnv)

    // 注释： 通过 rpcEnv 的 setupEndpoint 方法来启动 RpcEndpoint
    // 注释： rpcEnv = actorySystem,  setupEndpoint = actorOf,  helloEndpoint = actor
    rpcEnv.setupEndpoint(HelloRpcSettings.getName(), helloEndpoint)

    rpcEnv.awaitTermination()
  }
}