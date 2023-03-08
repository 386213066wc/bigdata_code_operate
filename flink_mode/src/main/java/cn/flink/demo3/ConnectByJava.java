package cn.flink.demo3;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class ConnectByJava {
    //将两个流式数据给union起来进行合并

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> arr1 = new ArrayList<>();
        ArrayList<Integer> arr2 = new ArrayList<>();
        arr1.add("hello world");
        arr1.add("spark flink");

        arr2.add(1);
        arr2.add(1);


        DataStreamSource<String> arrStream1 = executionEnvironment.fromCollection(arr1);
        DataStreamSource<Integer> arrStream2 = executionEnvironment.fromCollection(arr2);

        ConnectedStreams<String, Integer> connectedStream = arrStream1.connect(arrStream2);

        SingleOutputStreamOperator<String> stringStream = connectedStream.flatMap(new CoFlatMapFunction<String, Integer, String>() {

            @Override
            public void flatMap1(String s, Collector<String> collector) throws Exception {
                collector.collect(s);

            }

            @Override
            public void flatMap2(Integer integer, Collector<String> collector) throws Exception {
                collector.collect(integer+"");

            }
        });
        stringStream.print();
        executionEnvironment.execute();
    }
}