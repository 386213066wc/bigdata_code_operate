package cn.flink.demo1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlinkFileCountByJava {


    private static Tuple2<String, Integer> tuple2 = new Tuple2<String,Integer>();

    public static void main(String[] args) throws Exception {
        //获取程序执行的入口类
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> fileSource = executionEnvironment.readTextFile("file:///E:\\1、课程内容\\1、课程资料\\11、Flink实时处理\\2、数据资料\\hello.txt");
        FlatMapOperator<String, String> words = fileSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] arrays = line.split(" ");
                for (String array : arrays) {
                    collector.collect(array);
                }
            }
        });
        //使用new  接口的方式，使用匿名内部类来实现了 类的定义
        MapOperator<String, Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                System.out.println(s);
                tuple2.setFields(s,1);
                return tuple2;
            }
        });
        AggregateOperator<Tuple2<String, Integer>> resultSum = wordAndOne.groupBy(0).sum(1);
        resultSum.print();

    }
}
