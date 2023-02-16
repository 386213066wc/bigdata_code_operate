package cn.spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//定义一个样例类
//case class Person(id:String,name:String,age:Int)

object SparkDSL {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("sparkDSL")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc: SparkContext = sparkSession.sparkContext
    sc.setLogLevel("WARN")
    //加载数据
    val rdd1=sc.textFile(this.getClass.getClassLoader.getResource("person.txt").getPath).map(x=>x.split(" "))

    //把rdd与样例类进行关联
    val personRDD=rdd1.map(x=>Person(x(0),x(1),x(2).toInt))
    //把rdd转换成DataFrame
    import sparkSession.implicits._  // 隐式转换

    val personDF=personRDD.toDF
    //打印schema信息
    personDF.printSchema
    //展示数据
    personDF.show

    //查询指定的字段
    personDF.select("name").show
    personDF.select($"name").show
    //实现age+1
    personDF.select($"name",$"age",$"age"+1).show()

    //实现age大于30过滤
    personDF.filter($"age" > 30).show

    //按照age分组统计次数
    personDF.groupBy("age").count.show

    //按照age分组统计次数降序
    personDF.groupBy("age").count().sort($"age".desc).show
    sparkSession.stop()
    sc.stop()
  }
}