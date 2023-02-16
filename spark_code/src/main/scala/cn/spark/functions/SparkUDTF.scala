package cn.spark.functions

import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUDTF {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[8]").setAppName("sparkCSV")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val df: DataFrame = spark
      .read
      .format("csv")
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("header", "true")
      .option("multiLine", true)
      .load("E:\\1、课程内容\\1、课程资料\\9、Spark一站式解决方案\\2、SparkSQL结构化数据处理模块\\2、数据资料\\数据资料\\深圳链家二手房成交明细\\深圳链家二手房成交明细.csv")

    df.createOrReplaceTempView("house_sale")

    //注册utdf算子，这里无法使用sparkSession.udf.register()，注意包全路径
    spark.sql("CREATE TEMPORARY FUNCTION MySplit as 'cn.spark.functions.MySplit'")

    spark.sql("select part_place,MySplit(part_place,' ') from house_sale limit 50").show()
    spark.stop()
  }
}

class MySplit extends GenericUDTF {

  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    //判断参数是否为2
    if (args.length != 2) {
      throw new UDFArgumentLengthException("UserDefinedUDTF takes only two argument")
    }
    //判断第一个参数是不是字符串参数
    if (args(0).getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentException("UserDefinedUDTF takes string as a parameter")
    }

    //列名，会被用户传递的覆盖
    val fieldNames: ArrayList[String] = new ArrayList[String]()
    fieldNames.add("col1")

    //返回列以什么格式输出，这里是string，添加几个就是几个列，和上面的名字个数对应个数。
    var fieldOIs: ArrayList[ObjectInspector] = new ArrayList[ObjectInspector]()
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs)
  }

  override def process(objects: Array[AnyRef]): Unit = {
    //获取数据
    val data: String = objects(0).toString
    //获取分隔符
    val splitKey: String = objects(1).toString()
    //切分数据
    val words: Array[String] = data.split(splitKey)

    //遍历写出
    words.foreach(x => {
      //将数据放入集合
      var tmp: Array[String] = new Array[String](1)
      tmp(0) = x
      //写出数据到缓冲区
      forward(tmp)
    })
  }

  override def close(): Unit = {
    //没有流操作
  }
}
