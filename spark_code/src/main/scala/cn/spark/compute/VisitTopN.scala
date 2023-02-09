package cn.spark.compute

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

//对每个url计数；然后按次数降序排序；再取头5个
object VisitTopN {
  val  a:String = "20"

  def main(args: Array[String]): Unit = {

    //1、创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("VisitTopN").setMaster("local[2]")
    //2、创建SparkContext对象
    val sc = new SparkContext(sparkConf)


    //对spark程序某一时刻做快照
    sc.setCheckpointDir("hdfs://bigdata01:8020/spark1/checkpoint")



    sc.setLogLevel("warn")

    //3、读取数据文件
    val dataRDD: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("access.log").getPath)

    //4、先对数据进行过滤
    val filterRDD: RDD[String] = dataRDD.filter(x => x.split(" ").length > 10)

    filterRDD.checkpoint()
    //5、获取每一个条数据中的url地址链接
    val urlsRDD: RDD[String] = filterRDD.map(x => x.split(" ")(10))

    filterRDD.flatMap(x => x.split(" "))

    //6、过滤掉不是http的请求
    val fUrlRDD: RDD[String] = urlsRDD.filter(_.contains("http"))

    //对rdd数据进行缓存操作  将rdd数据给存放到内存当中去进行缓存
    fUrlRDD.cache()  //就是调用了 persist()

    //通过storageLevel来指定数据的存储级别  指定堆外内存  StorageLevel.OFF_HEAP  系统内存
   // fUrlRDD.persist(StorageLevel.OFF_HEAP)

    //如果缓存的数据丢失了怎么办？？？
    fUrlRDD.persist(StorageLevel.MEMORY_AND_DISK_SER_2)

    //取消RDD的数据缓存
  //  fUrlRDD.unpersist(true)


    //7、把每一个url计为1
    val urlAndOneRDD: RDD[(String, Int)] = fUrlRDD.map(x => (x, 1))

    //8、相同的url出现1进行累加
    val result: RDD[(String, Int)] = urlAndOneRDD.reduceByKey(_ + _)

    //9、对url出现的次数进行排序----降序
    val sortRDD: RDD[(String, Int)] = result.sortBy(_._2, false)

    //10、取出url出现次数最多的前5位
    val top5: Array[(String, Int)] = sortRDD.take(5)
    top5.foreach(println)
    sc.stop()
  }
}