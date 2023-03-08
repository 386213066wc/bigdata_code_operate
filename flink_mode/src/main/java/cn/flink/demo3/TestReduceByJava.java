package cn.flink.demo3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TestReduceByJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, Integer>> arrayList = new ArrayList<>();
        for(int i = 0;i<6;i++){
            Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<String,Integer>();
            stringIntegerTuple2.setFields("a",i);
            arrayList.add(stringIntegerTuple2);
        }


        DataStreamSource<ArrayList<Tuple2<String, Integer>>> arrayListDataStreamSource = executionEnvironment.fromElements(arrayList);

        SingleOutputStreamOperator<Tuple2<String, Integer>> eachTuple = arrayListDataStreamSource.flatMap(new FlatMapFunction<ArrayList<Tuple2<String, Integer>>, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(ArrayList<Tuple2<String, Integer>> value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (Tuple2<String, Integer> stringIntegerTuple2 : value) {
                    out.collect(stringIntegerTuple2);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyByStream = eachTuple.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceResult = keyByStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {

                Tuple2<String, Integer> stringIntegerTuple2 = new Tuple2<>();
                stringIntegerTuple2.setFields(value1.f0, value1.f1 + value2.f1);
                return stringIntegerTuple2;
            }
        });
        reduceResult.print();
        executionEnvironment.execute();
    }
}