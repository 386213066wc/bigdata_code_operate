package cn.spark.count

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CoffinChainCSV {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("SparkSQLOpt")
    conf.setMaster("local[1]")
    conf.set("spark.sql.adaptive.enabled","true")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //  # 读入csv文件，读取完后df的类型为dataframe
    //需求一：查看咖啡销量排名
    // val filePath=""
    val df=spark.read.options(
      Map("inferSchema"->"true","delimiter"->",","header"->"true")).csv("data/Coffee_Chain.csv")
    //   # 创建表用于sql查询
    df.createOrReplaceTempView("coffee")
    //  # 执行查询
    val b = spark.sql("select product,sum(Marketing) as number from coffee group by product order by number desc")
    //  # 保存文件为sell_num.csv
    b.write.option("header",true).csv("file:///C:\\out_result\\out1")



    //统计咖啡销量的分布情况

    //（1）查询咖啡销售量和state的关系。
    //  #  //可以发现，比较有钱的城市（加利福尼亚和纽约）最喜欢喝咖啡。
    val sales = spark.sql("select market,state,sum(Marketing) as number from coffee group by state,market order by number desc")
    // # 保存文件为csv
    sales.write.option("header",true).csv("file:///C:\\out_result\\out2")


    // （2）查询咖啡销售量和market销售关系。

    val salesAndMarket = spark.sql("select market,sum(Marketing) as number from coffee group by market order by number desc")
    //  # 保存文件为csv  可以发现西部地区和中部地区消费咖啡的量远远大于东部和南部。
    salesAndMarket.write.option("header",true).csv("file:///C:\\out_result\\out3")

    //  （3）查询咖啡的平均利润和售价。
    val profit_avg = spark.sql("select product,avg(`Coffee Sales`),avg(profit) as avg_profit from coffee group by product order by avg_profit desc")
    //    # 保存文件为csv
    profit_avg.write.option("header",true).csv("file:///C:\\out_result\\out4")

    //   （4）查询咖啡的平均利润和售价和销售量的关系。
    val avgSales = spark.sql("select a.product,avg(`Coffee Sales`),avg(profit) as avg_profit , b.number from coffee as a, (select product,sum(Marketing) as number from coffee group by product) as b where a.product == b.product group by a.product,b.number order by b.number desc ")
    //  # 保存文件为csv  我们发现售价和利润与销售额似乎没有特别明显的关系。更进一步的，我们可以看看是否销售额高的商品，其他成本（人力成本抑或是广告成本）更高？
    avgSales.write.option("header",true).csv("file:///C:\\out_result\\out5")


    //（5）查询咖啡的平均利润、销售量与其他成本的关系。
    // # 执行查询
    val profit_avg_sales = spark.sql("select a.product, avg(profit) as avg_profit , b.number , avg(a.`Total Expenses`) as other_cost from coffee as a, (select product,sum(Marketing) as number from coffee group by product) as b where a.product == b.product group by a.product,b.number order by b.number desc ")
    //   # 保存文件为csv 果然，销售额好的商品，他们的其他支出较高，极有可能是花了较多的钱去做广告或是雇佣更好的工人做咖啡，抑或是有着更好的包装。
    profit_avg_sales.write.option("header",true).csv("file:///C:\\out_result\\out6")


    //（6）查询咖啡属性与平均售价、平均利润、销售量与其他成本的关系。
    //  # 执行查询
    val avg_sales_profit_cost = spark.sql("select a.type,avg(a.`Coffee Sales`) as avg_sales, avg(profit) as avg_profit , sum(b.number) as total_sales , avg(a.`Total Expenses`) as other_cost from coffee as a, (select product,sum(Marketing) as number from coffee group by product) as b where a.product == b.product group by a.type order by total_sales desc ")
    //  # 保存文件为csv  可以发现，这两种咖啡的平均售价和利润不会差的很多，其他花费也差不多，但是销售量却差了近七百万，原因可能在于Decaf的受众不多，Decaf为低因咖啡，Regular为正常的咖啡。
    avg_sales_profit_cost.write.option("header",true).csv("file:///C:\\out_result\\out7")

    //（7）查询市场规模、市场地域与销售量的关系。
    // # 执行查询
    val marketSize_sales = spark.sql("select Market,`Market Size`, sum(`Coffee Sales`) as total_sales  from coffee group by Market,`Market Size` order by total_sales desc ")
    //  # 保存文件为csv  可以发现，小商超卖的咖啡竟然会比大商超卖的数量更多！
    marketSize_sales.write.option("header",true).csv("file:///C:\\out_result\\out8")
  }


}