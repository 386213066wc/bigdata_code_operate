package cn.spark.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1: RDD[String] = sc.parallelize(Array("hello", "hadooop", "hello", "spark"), 1)
    val rdd2: RDD[(String, Int)] = rdd1.map((_, 1))
    println(rdd2.partitions.length) //1
    println(rdd2.partitioner) //None
    println()

    val rdd3: RDD[(String, Int)] = rdd2.partitionBy(new MyPatitioner(2))
    println(rdd3.partitions.length) //2
    println(rdd3.partitioner) //Some(com.kkb.spark.demo.MyPatitioner@41c204a0)
    println()

    val result: RDD[(Int, (String, Int))] = rdd3.mapPartitionsWithIndex((index, iter) => {
      iter.map(x => (index, (x._1, x._2)))
    })
    result.foreach(println)

    sc.stop()

    /**
     * (0,(hadooop,1))
     * (1,(hello,1))
     * (0,(spark,1))
     * (1,(hello,1))
     **/
  }
}

/**
 *
 * @param num 分区数
 */
class MyPatitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    System.identityHashCode(key) % num.abs
  }
}