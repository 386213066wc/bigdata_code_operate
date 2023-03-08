package cn.flink.demo3;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class UnionByJava {
    //将两个流式数据给union起来进行合并

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> arr1 = new ArrayList<>();
        ArrayList<String> arr2 = new ArrayList<>();
        arr1.add("hello world");
        arr1.add("spark flink");

        arr2.add("hello world");
        arr2.add("hive spark");


        DataStreamSource<String> arrStream1 = executionEnvironment.fromCollection(arr1);
        DataStreamSource<String> arrStream2 = executionEnvironment.fromCollection(arr2);

        DataStream<String> union = arrStream1.union(arrStream2);
        union.print();
        executionEnvironment.execute();
    }
}