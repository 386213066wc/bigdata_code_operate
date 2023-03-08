package cn.flink.demo5;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

public class DistinctDataSetByJava {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> sourceStream = executionEnvironment.fromElements("hello world1", "hello world2", "hello world3", "hello world4");

        FlatMapOperator<String, String> eachWord = sourceStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] valueArr = value.split(" ");
                for (String eachWord : valueArr) {
                    out.collect(eachWord);
                }
            }
        });
        //对每个单词进行去重
        DistinctOperator<String> distinctStream = eachWord.distinct();
        distinctStream.print();
    }
}