package cn.flink.demo11;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class AggregateTimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketTextStream = environment.socketTextStream("bigdata01", 9999);
        Tuple2<String, Integer> tuple2 = new Tuple2<String,Integer>();
        socketTextStream.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }).map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2 map(String value) throws Exception {
                        tuple2.setFields(value,1);
                        return tuple2;
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {

                    private Tuple2 initAccumulator = new Tuple2("",0);

                    //表示我们初始化的累加器
                    @Override
                    public Tuple2<String, Integer> createAccumulator() {
                        return initAccumulator;
                    }

                    //每次的数据的累加的操作
                    @Override
                    public Tuple2<String, Integer> add(Tuple2<String, Integer> value, Tuple2<String, Integer> accumulator) {
                        Tuple2<String, Integer> tuple21 = new Tuple2<>(value.f0, accumulator.f1 + value.f1);
                        return tuple21;
                    }

                    //获取累加器的累加结果
                    @Override
                    public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                        return accumulator;
                    }

                    //合并最终的结果
                    @Override
                    public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                        return  new Tuple2<String,Integer>(a.f0,a.f1 + b.f1);
                    }
                })
                .print();
        environment.execute();
    }
}