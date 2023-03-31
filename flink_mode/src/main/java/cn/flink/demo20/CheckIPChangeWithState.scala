package cn.flink.demo20


import java.util
import java.util.Collections
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.log4j.{Level, Logger}
/**
 * todo: 使用state编程进行代码实现进行ip检测
 */

case class UserLogin(ip:String,  username:String,  operateUrl:String,  time:String)

object CheckIPChangeWithState {
  /*
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //todo:1、接受socket数据源
    val sourceStream: DataStream[String] = environment.socketTextStream("bigdata01",9999)

    //todo:2、数据处理        // 192.168.52.100,zhubajie,https://icbc.com.cn/login.html,2020-02-12 12:23:45
    sourceStream.map(x =>{
      val strings: Array[String] = x.split(",")
      (strings(1),UserLogin(strings(0),strings(1),strings(2),strings(3)))
    })
      .keyBy(1)
      .process(new LoginCheckProcessFunction)
      .print()

    //todo: 3、启动任务
    environment.execute("checkIpChange")

  }
}
//todo: 自定义KeyedProcessFunction类
class LoginCheckProcessFunction extends KeyedProcessFunction[String,(String,UserLogin),(String,UserLogin)]{

  //todo: 定义ListState
  var listState:ListState[UserLogin]=_

  //todo: 初始化方法
  override def open(parameters: Configuration): Unit = {
    //listState赋值
    listState = getRuntimeContext.getListState(new ListStateDescriptor[UserLogin]("changeIp",classOf[UserLogin]))

  }

  //todo: 解析用户访问信息
  override def processElement(value: (String, UserLogin), ctx: KeyedProcessFunction[String, (String, UserLogin), (String, UserLogin)]#Context, out: Collector[(String, UserLogin)]): Unit = {
    val logins = new util.ArrayList[UserLogin]()
    //添加到list集合
    listState.add(value._2)

    import scala.collection.JavaConverters._
    val toList: List[UserLogin] = listState.get().asScala.toList
    //排序
    val sortList: List[UserLogin] = toList.sortBy(_.time)

    if(sortList.size ==2){
      val first: UserLogin = sortList(0)
      val second: UserLogin = sortList(1)

      if(!first.ip.equals(second.ip)){
        println("小伙子你的IP变了，赶紧回去重新登录一下")
      }
      //todo: 移除第一个ip，保留第二个ip
      logins.removeAll(Collections.EMPTY_LIST)
      //添加第一个ip地址
      logins.add(second)
      //更新状态
      listState.update(logins)
    }

    //输出
    out.collect(value)

  }*/

}