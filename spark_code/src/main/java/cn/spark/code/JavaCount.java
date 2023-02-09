package cn.spark.code;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class JavaCount {
    public static void main(String[] args) {/*
        //获取SparkCOntext
        SparkConf sparkConf = new SparkConf().setAppName("sparkCount")
                .setMaster("local[*]");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        int a = 20;

        //读取数据  通过rdd来划分stage，切分task都是在Driver端执行的，executor里面执行的是序列化之后的task
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://bigdata01:8020/count.txt");

        //进行切分
        JavaRDD<String> allWord = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] s1 = s.split(" ");
                return Arrays.asList(s1).iterator();
            }
        });


        //出现一次记作一次

        JavaPairRDD<String, Integer> wordAndOne = allWord.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {

                return new Tuple2<String, Integer>(word, 1);
            }
        });


        //按照key进行聚合统计

        JavaPairRDD<String, Integer> wordAndTotal = wordAndOne.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        Tuple2<String, String> stringStringTuple2 = new Tuple2<>("hello", "world");
        //调用了collect之后，收集回来的数据，存放在Driver端，默认最大收集1GB数据，超过1GB，丢掉，但是1GB的数据，Driver端能不能存的下
        List<Tuple2<String, Integer>> collect = wordAndTotal.collect();
        for (Tuple2<String, Integer> stringIntegerTuple2 : collect) {
            System.out.println(stringIntegerTuple2._1);
            System.out.println(stringIntegerTuple2._2);
        }
        javaSparkContext.stop();*/
    }
}
