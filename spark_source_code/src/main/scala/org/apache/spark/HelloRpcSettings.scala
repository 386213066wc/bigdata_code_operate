package org.apache.spark

/*************************************************
 *
 *  注释： 配置类
 */
object HelloRpcSettings {
  // 注释： RPC 组件的名称，绑定端口，主机名称等
  val rpcName = "hello-rpc-service"
  val port = 9527
  val hostname = "localhost"

  def getName() = {
    rpcName
  }
  def getPort(): Int = {
    port
  }
  def getHostname(): String = {
    hostname
  }
}