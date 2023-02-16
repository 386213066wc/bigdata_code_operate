package cn.spark.sparksql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo1 {

  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder()
      .appName("demo1")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()

    val df: DataFrame = spark.read.text(this.getClass.getClassLoader.getResource("person.txt").getPath)
    //Prints the schema to the console in a nice tree format
    //打印schema信息
    df.printSchema

    /**
     * root
     * |-- value: string (nullable = true)
     **/
    println("-----")
    //Returns the number of rows in the Dataset
    //返回df有多少行
    println(df.count())

    /** 4 */
    println("-----")
    //返回头20行
    df.show()

    /**
     * +------------+
     * |       value|
     * +------------+
     * | 1 youyou 38|
     * |   2 Tony 25|
     * |3 laowang 18|
     * |   4 dali 30|
     * +------------+
     **/
    spark.stop()
  }

}
