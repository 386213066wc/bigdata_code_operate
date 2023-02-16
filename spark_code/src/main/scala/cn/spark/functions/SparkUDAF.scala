package cn.spark.functions

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object SparkUDAF{
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    val frame: DataFrame = session
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("E:\\1、课程内容\\1、课程资料\\9、Spark一站式解决方案\\2、SparkSQL结构化数据处理模块\\2、数据资料\\数据资料\\深圳链家二手房成交明细\\深圳链家二手房成交明细.csv")
    frame.createOrReplaceTempView("house_sale")
    session.sql("select floor from house_sale limit 30").show()
    session.udf.register("udaf",new MyAverage)
    session.sql("select floor, udaf(house_sale_money) from house_sale group by floor").show()
    frame.printSchema()
    session.stop()
  }
}

class MyAverage extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据类型
  def inputSchema: StructType = StructType(StructField("floor", DoubleType) :: Nil)

  // 聚合缓冲区中值得数据类型
  def bufferSchema: StructType = {
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)
  }

  // 返回值的数据类型
  def dataType: DataType = DoubleType

  // 对于相同的输入是否一直返回相同的输出。
  def deterministic: Boolean = true

  // 初始化
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 用于存储不同类型的楼房的总成交额
    buffer(0) = 0D
    // 用于存储不同类型的楼房的总个数
    buffer(1) = 0L
  }

  // 相同Execute间的数据合并。（分区内聚合）
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 不同Execute间的数据合并（分区外聚合）
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  def evaluate(buffer: Row): Double = buffer.getDouble(0) / buffer.getLong(1)
}