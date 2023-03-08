package cn.flink.demo2;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class StreamingSourceFromCollectionByJava {
    public static void main(String[] args) throws Exception {
        //流式数据处理程序的入口类
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //集合当中的数据的处理
        ArrayList<String> arrayList = new ArrayList<>();

        arrayList.add("hello world");
        arrayList.add("spark flink");
        arrayList.add("test spark");
        arrayList.add("flink hive");
        arrayList.add("count now");

        DataStreamSource<String> dataStreamSource = executionEnvironment.fromCollection(arrayList);

        SingleOutputStreamOperator<String> words = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] wordsArray = value.split(" ");
                for (String word : wordsArray) {
                    out.collect(word);
                }
            }
        });

        Tuple2<String, Integer> tuple2 = new Tuple2<>();

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                tuple2.setFields(value, 1);

                return tuple2;
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultSum = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);
        resultSum.print();
        executionEnvironment.execute();
    }
}