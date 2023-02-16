package org.apache.spark


import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}

/**
 *
 *  注释： RPC 服务组件
 */
class HelloEndPoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {


  // 注释： 在实例被构造出来的时候，自动执行一次
  // 注释： 类似于 akka 中的 actor 里面的 preStart()
  override def onStart(): Unit = {
    println(rpcEnv.address)
    println("Start HelloEndPoint")
  }

  // 注释： 服务方法
  override def receive: PartialFunction[Any, Unit] = {
    case SayHi(msg) => println(s"Receive Message: $msg")
  }
  // 注释： 服务方法
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case SayHi(msg) => {
      println(s"Receive Message: $msg")
      context.reply(s"I'm Server, $msg")
    }
    case SayBye(msg) => {
      println(s"Receive Message: $msg")
      context.reply(s"I'm Server, $msg")
    }
  }
  override def onStop(): Unit = {
    println("Stop HelloEndPoint")
  }
}
// 注释： 消息类
case class SayHi(msg: String)
// 注释： 消息类
case class SayBye(msg: String)