package cn.spark.compute

import java.sql.{Connection, DriverManager}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

case class Job_Detail(job_id:String, job_name:String, job_url:String,job_location:String, job_salary:String,
                      job_company:String,job_experience:String,job_class:String,job_given:String,
                      job_detail:String, company_type:String,company_person:String ,
                      search_key:String, city:String)

object JdbcOperate {
  //定义一个函数，无参，返回一个jdbc的连接（用于创建JdbcRDD的第二个参数）
  val getConn: () => Connection = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8","root","root")
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRddDemo").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)

    //创建RDD，这个RDD会记录以后从MySQL中读数据
    val jdbcRDD: JdbcRDD[Job_Detail] = new JdbcRDD(
      sparkContext,            //SparkContext
      getConn,        //返回一个jdbc连接的函数
      "SELECT * FROM jobdetail WHERE job_id >= ? AND job_id <= ?",  //sql语句（要包含两个占位符）
      1,      //第一个占位符的最小值
      75000,    //第二个占位符的最大值
      8, //分区数量
      rs => {
        val job_id = rs.getString(1)
        val job_name: String = rs.getString(2)
        val job_url = rs.getString(3)
        val job_location: String = rs.getString(4)
        val job_salary = rs.getString(5)
        val job_company: String = rs.getString(6)
        val job_experience = rs.getString(7)
        val job_class: String = rs.getString(8)
        val job_given = rs.getString(9)
        val job_detail: String = rs.getString(10)
        val company_type = rs.getString(11)
        val company_person: String = rs.getString(12)
        val search_key = rs.getString(13)
        val city: String = rs.getString(14)
        Job_Detail(job_id, job_name, job_url,job_location, job_salary, job_company,job_experience,job_class,job_given,job_detail, company_type,company_person ,search_key, city)
      }
    )

    val searchKey: RDD[(String, Iterable[Job_Detail])] = jdbcRDD.groupBy(x => x.search_key)

    //需求一：求取每个搜索关键字下的职位数量，并将结果入库mysql，注意：实现高效入库

    //第一种实现方式
    val searchKeyRdd: RDD[(String, Int)] = searchKey.map(x => (x._1,x._2.size))
    //第二种实现方式
    //求取每个搜索关键字出现的岗位人数
    val resultRdd: RDD[(String, Int)] = jdbcRDD.map(x => (x.search_key,1)).reduceByKey(_ + _).filter(x => x._1 != null)
    //数据量变少，缩减分区个数
    val rsultRdd2: RDD[(String, Int)] = resultRdd.coalesce(2)
    //将统计的结果写回去mysql
    rsultRdd2.foreachPartition( iter =>{ //创建数据库连接
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8", "root", "root")
      //关闭自动提交
      conn.setAutoCommit(false);
      val statement = conn.prepareStatement("insert into job_count(search_name, job_num) values (?, ?)")
      //遍历
      iter.foreach(record =>{
        //赋值操作
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        //添加到一个批次中
        statement.addBatch()
      })
      //批量提交该分区所有数据
      statement.executeBatch()
      conn.commit()
      conn.close()
      // 关闭资源
      statement.close()
      //利用连接池，使用完了就回收
    })
    //需求二，求取每个搜索关键字岗位下最高薪资的工作信息，以及最低薪资下的工作信息
    val getEachJobs: RDD[(String, Iterable[Job_Detail])] = jdbcRDD.groupBy(x => x.search_key)
    val maxJobDetail: RDD[Job_Detail] = getEachJobs.map(x => {

      val value: Iterable[Job_Detail] = x._2
      val array: Array[Job_Detail] = value.toArray
      array.maxBy(x => {
        val job_salary: String = x.job_salary
        val result = if (StringUtils.isNotEmpty(job_salary) && job_salary.contains("k") && job_salary.contains("-") && job_salary.replace("k", "").split("-").length >= 2) {
          val strings: Array[String] = job_salary.replace("k", "").split("-")
          val result2 = if (strings.length >= 2) {
            strings(1).toInt
          } else {
            0
          }
          result2
        } else {
          0
        }
        result
      })
    })
    val details: Array[Job_Detail] = maxJobDetail.collect()
    details.foreach(x =>{
      println(x.job_id + "\t" + x.job_salary + "\t" + x.search_key + "\t" + x.job_company)
    })
    sparkContext.stop()
  }
}